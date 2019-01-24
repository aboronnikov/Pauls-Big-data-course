package com.epam.utilities

import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Kafka input/output utilities for this program.
 */
object KafkaIOUtils {
  /**
   * Reads a kafka topic tweet stream into a streaming data frame.
   *
   * @param spark           spark session used.
   * @param bootstrapServer bootstrap server url.
   * @param inputTopic      the topic to read from.
   * @param startingOffsets starting offsets of the input topic.
   * @return
   */
  def readStreamIntoDataFrame(spark: SparkSession,
                              bootstrapServer: String,
                              inputTopic: String,
                              startingOffsets: String): DataFrame = {
    import com.epam.extensions.SparkExtensions._
    import spark.implicits._
    spark.readStreamFromKafka(bootstrapServer, inputTopic, startingOffsets)
      .flatMap(JsonUtils.transformTweetStringIntoObjects)
      .map(tweet => (tweet.dateTime, tweet.hashTag, tweet.userId))
      .toDF("dateTime", "hashTag", "userId")
      .selectExpr("CAST(dateTime as TimeStamp)", "hashTag", "userId")
  }

  /**
   * Streams data into another kafka topic.
   *
   * @param spark              spark session to be used.
   * @param streamingDf        streaming df to write into the topic.
   * @param checkPointLocation location of the checkpointing folder.
   * @param bootstrapServer    bootstrap server's url.
   * @param outputTopic        the topic to write into.
   * @return a streaming query.
   */
  def streamIntoKafka(spark: SparkSession,
                      streamingDf: DataFrame,
                      checkPointLocation: String,
                      bootstrapServer: String,
                      outputTopic: String): StreamingQuery = {
    import com.epam.extensions.DataFrameExtensions._
    import spark.implicits._
    streamingDf
      .select(to_json(struct($"window", $"hashTag", $"userId", $"count")).alias("value"))
      .writeStreamToKafka(checkPointLocation, bootstrapServer, outputTopic, OutputMode.Update())
      .start()
  }
}
