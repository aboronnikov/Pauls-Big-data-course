package com.epam.spark.extensions

import com.epam.processingutils.JsonUtils
import com.epam.spark.extensions.DataFrameExtensions._
import com.epam.spark.extensions.FileSystemExtensions._
import com.epam.spark.extensions.RowExtensions._
import com.epam.spark.extensions.SparkDataFrameExtensions._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.language.implicitConversions

/**
  * A library used to read, write, remove and combine dataframes.
  */
object SparkExtensions {

  /**
    * This is the schema specifying all the fields we need to store into the hdfs.
    */
  val TweetSchema: StructType =
    new StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("hour", StringType, nullable = true),
      StructField("hashTag", StringType, nullable = true),
      StructField("userId", StringType, nullable = true),
      StructField("cnt", IntegerType, nullable = true)
    ))

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param spark the extended sparkSession object.
    * @return RichSpark object with extensions.
    */
  implicit def richSpark(spark: SparkSession): RichSpark = new RichSpark(spark)

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param spark the extended sparkSession object
    */
  class RichSpark(spark: SparkSession) {

    /**
      * Loads a partition of data from the HDFS.
      *
      * @param basePath base path to the partitions, e.g. /user/maria_dev/test.
      * @param path     full path to partitioned data, e.g. /user/maria_dev/test/key1/key2.
      * @param format   the data format we are working with.
      * @return
      */
    private def loadDFPartition(basePath: String, path: String, format: String): DataFrame = {
      spark.read
        .schema(TweetSchema)
        .format(format)
        .option("basePath", basePath)
        .load(path)
        .unionCompatible
    }

    /**
      * Reads the old dataframe from hdfs, to later merge it with the new dataframe, made up from kafka.
      *
      * Here load means move to another HDFS location (I avoid using RAM here).
      *
      * @param format   format.
      * @param basePath path.
      * @return dataframe, read from the hdfs.
      */
    def loadDataFrameFromHDFS(format: String,
                              basePath: String,
                              partitions: Array[String],
                              fs: FileSystem): DataFrame = {
      val df = partitions
        .filter(path => fs.doesPathExist(basePath, path)) // drop unused paths.
        .map(path => loadDFPartition(basePath, basePath + path, format)) // load the corresponding DataFrame.
        .fold(spark.emptyTypedDataFrame)(_.union(_)) // union dfs into one, returning an empty one, in case of no such dfs.

      val persistedDF = df.persistEagerly
      // remove partitions to be able to append later on
      partitions.foreach(path => {
        fs.removePathIfExists(basePath, path)
      })
      persistedDF
    }

    /**
      * Reads kafka messages from a particular topic, starting at one offset and finishing at another.
      *
      * 1) Read data from kafka.
      *
      * 2) Transform (using flatMap) a tweet with multiple hashtags into multiple tweets with just one hashtag,
      * so, this hypothetical tweet: {message: "bla", hashtags: ["one", "two", "three"]}
      * will be transformed into this array of tweets:
      * [{message: "bla", hashtag: "one"}, {message: "bla", hashtag: "two"}, {message: "bla", hashtag: "three"}]
      *
      * 3) Tweets from Dataset[TweetWithKeys] are mapped into DataFrames with corresponding column names.
      *
      * 4) Then we group these tweets by "date", "hour", "hashTag", "userId" and tally them up.
      *
      * @param bootstrapServer the url of the bootstrap server.
      * @param topicName       name of the topic to read from in kafka.
      * @param startingOffsets starting offsets of messages within the topic.
      * @param endingOffsets   ending offsets of messages within the topic.
      * @return a dataframe containing read data.
      */
    def readDataFrameFromKafka(bootstrapServer: String,
                               topicName: String,
                               startingOffsets: String,
                               endingOffsets: String): DataFrame = {
      import org.apache.spark.sql.functions.count
      import spark.implicits._
      import com.epam.spark.extensions.DataFrameReaderExtensions._

      spark.read
        .fromKafka(spark, bootstrapServer, topicName, startingOffsets, endingOffsets)
        .flatMap(value => JsonUtils.transformTweetStringIntoObjects(value))
        .map(tweet => (tweet.date, tweet.hour, tweet.hashTag, tweet.userId))
        .toDF("date", "hour", "hashTag", "userId")
        .groupBy("date", "hour", "hashTag", "userId")
        .agg(count("*").alias("cnt"))
    }

    /**
      * Merges the old data DataFrame with the new data one.
      *
      * 1) Appends the rows of one dataframe to another.
      *
      * 2) Groups by "date", "hour", "hashTag", "userId"
      *
      * 3) Sums the old counts with the new ones.
      *
      * @param oldDF one dataframe (e.g. the one you read from hdfs)
      * @param newDF another dataframe (e.g. the one you read from kafka)
      * @return a combined dataframe with rows of one dataframe appended to the other.
      */
    def mergeRunningTotals(oldDF: DataFrame, newDF: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions.sum
      val unified = newDF.unionCompatible.union(oldDF.unionCompatible)
      unified
        .groupBy("date", "hour", "hashTag", "userId")
        .agg(sum("cnt").alias("cnt"))
    }

    /**
      * Extract paths that we will need to use to load data from the HDFS.
      * This method is written to be able to identify what existing partitions will be in use when
      * merging with new data from kafka.
      *
      * @param df the dataframe just read from kafka.
      * @return an array on paths.
      */
    def extractIntersectionPaths(df: DataFrame): Array[String] = {
      df
        .select("date", "hour", "hashTag", "userId")
        .rdd
        .map(_.extractValueString)
        .collect() // this inexpensive collect here is done on purpose, to avoid the RDD[RDD] scenario later on.
    }

    /**
      * Writes a particular dataframe to the specified path in the specified format.
      *
      * @param dataFrame dataframe to write into the hdfs.
      * @param path      path to write this dataframe to.
      * @param format    format in which to write this dataframe into the hdfs.
      */
    def writeDataFrameToHDFS(dataFrame: DataFrame, path: String, format: String): Unit = {
      dataFrame
        .write
        .mode(SaveMode.Append)
        .partitionBy("date", "hour", "hashTag", "userId")
        .option("header", "true")
        .format(format)
        .save(path)
    }
  }

}
