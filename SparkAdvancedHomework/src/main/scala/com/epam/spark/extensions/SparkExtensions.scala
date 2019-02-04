package com.epam.spark.extensions

import scala.language.implicitConversions

/**
 * A library used to read, write, remove and combine dataframes.
 */
object SparkExtensions {

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
    private def loadDFPartition(basePath: String,
                                path: String,
                                format: String,
                                schema: StructType): DataFrame = {
      spark.read
        .schema(schema)
        .format(format)
        .option("basePath", basePath)
        .load(path)
    }

    /**
     * Reads data from the HDFS into a dataframe. (I avoid using RAM here)
     *
     * @param format     format that the data is stored in.
     * @param basePath   path to the data directory.
     * @param partitions partitions to be read from.
     * @param fs         fileSystem object to use when check for path existence.
     * @return DataFrame with the data from HDFS.
     */
    def readDataFrameFromHDFS(format: String,
                              basePath: String,
                              partitions: Array[String],
                              fs: FileSystem,
                              schema: StructType): DataFrame = {
      partitions
        .filter(path => fs.doesPathExist(basePath, path)) // drop unused paths.
        .map(path => loadDFPartition(basePath, basePath + path, format, schema)) // load the corresponding DataFrame.
        .fold(spark.emptyTypedDataFrame(schema))(_.unionByName(_)) // union dfs, returning an empty one, in case of no dfs.
        .persistEagerly // persist this dataframe to disk
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
     * @return a DataFrame containing read data.
     */
    def readTweetDataFrameFromKafka(bootstrapServer: String,
                                    topicName: String,
                                    startingOffsets: String,
                                    endingOffsets: String,
                                    columnNames: Array[String],
                                    countColumnName: String): DataFrame = {

      val columns = columnNames.toList.map(name => new Column(name))
      spark.read
        .fromKafka(spark, bootstrapServer, topicName, startingOffsets, endingOffsets)
        .flatMap(value => JsonUtils.transformTweetStringIntoObjects(value))
        .map(tweet => (tweet.date, tweet.hour, tweet.hashTag, tweet.userId))
        .toDF(columnNames: _*)
        .groupBy(columns: _*)
        .agg(count("*").alias(countColumnName))
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
    def mergeRunningTotals(oldDF: DataFrame, newDF: DataFrame, groupByColumns: Array[String], countColumnName: String): DataFrame = {
      val unified = newDF.unionByName(oldDF)
      val typedColumns = groupByColumns.toList.map(name => new Column(name))
      unified
        .groupBy(typedColumns: _*)
        .agg(sum(countColumnName).alias(countColumnName))
    }

    /**
     * Writes a particular dataframe to the specified path in the specified format.
     *
     * @param dataFrame  dataframe to write into the hdfs.
     * @param path       path to write this dataframe to.
     * @param format     format in which to write this dataframe into the hdfs.
     * @param partitions partitions to be used when writing these data to the HDFS
     */
    def writeDataFrameToHDFS(dataFrame: DataFrame, path: String, format: String, partitions: Array[String], saveMode: SaveMode): Unit = {
      dataFrame
        .write
        .mode(saveMode)
        .partitionBy(partitions: _*)
        .format(format)
        .save(path)
    }

    /**
     * A shortcut method to create an dataframe with twitter schema.
     *
     * @param schema schema of the DataFrame to be created.
     * @return empty DataFrame with the specified schema.
     */
    def emptyTypedDataFrame(schema: StructType): DataFrame = {
      val emptyRDD = spark.emptyDataFrame.rdd
      spark.createDataFrame(emptyRDD, schema)
    }
  }

}
