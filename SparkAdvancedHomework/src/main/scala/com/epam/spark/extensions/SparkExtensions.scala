package com.epam.spark.extensions

import com.epam.processingutils.JsonUtils
import com.epam.spark.extensions.DataFrameExtensions._
import com.epam.spark.extensions.FileSystemExtensions._
import com.epam.spark.extensions.RowExtensions._
import com.epam.spark.extensions.SparkDataFrameExtensions._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
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
      StructField("cnt", DoubleType, nullable = true)
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
     * @param format   format.
     * @param basePath path.
     * @return dataframe, read from the hdfs.
     */
    def readOldDF(format: String, basePath: String, partitions: Array[String], fs: FileSystem): DataFrame = {
      val df = partitions
        .filter(path => fs.doesPathExist(basePath, path)) // drop unused paths.
        .map(path => loadDFPartition(basePath, basePath + path, format)) // load corresponding df.
        .fold(spark.emptyDF)(_.union(_)) // union dfs into one, returning an empty one, in case of no such dfs.
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
     * @param bootstrapServer the url of the bootstrap server.
     * @param topicName       name of the topic to read from in kafka.
     * @param startingOffsets starting offsets of messages within the topic.
     * @param endingOffsets   ending offsets of messages within the topic.
     * @return a dataframe containing read data.
     */
    def readNewDF(bootstrapServer: String,
                  topicName: String,
                  startingOffsets: String,
                  endingOffsets: String): DataFrame = {
      import org.apache.spark.sql.functions.count
      import spark.implicits._

      spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", topicName)
        .option("startingOffsets", startingOffsets)
        .option("endingOffsets", endingOffsets)
        .load()
        .selectExpr("CAST(value AS String)")
        .as[String]
        .flatMap(value => JsonUtils.transformTweetStringIntoObject(value))
        .map(tweet => (tweet.date, tweet.hour, tweet.hashTag, tweet.userId))
        .toDF("date", "hour", "hashTag", "userId")
        .groupBy("date", "hour", "hashTag", "userId")
        .agg(count("*").alias("cnt"))
    }

    /**
     * Appends the rows of one dataframe to another.
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
     *
     * @param df the dataframe just read from kafka.
     * @return an array on paths.
     */
    def extractPaths(df: DataFrame): Array[String] = {
      df
        .select("date", "hour", "hashTag", "userId")
        .rdd
        .map(_.extractValueString)
        .collect() // collect here is done on purpose
    }

    /**
     * Writes a particular dataframe to the specified path in the specified format.
     *
     * @param dataFrame dataframe to write into the hdfs.
     * @param path      path to write this dataframe to.
     * @param format    format in which to write this dataframe into the hdfs.
     */
    def writeToHDFS(dataFrame: DataFrame, path: String, format: String): Unit = {
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
