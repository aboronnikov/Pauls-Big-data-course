package com.epam.spark.extensions

import scala.language.implicitConversions

/**
 * Aids reading from disparate sources.
 */
object DataFrameReaderExtensions {

  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param dfReader this DataFrameReader.
   * @return RichDataFrameReader instance.
   */
  implicit def richDataFrameReader(dfReader: DataFrameReader): RichDataFrameReader = new RichDataFrameReader(dfReader)

  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param dfReader This DataFrameReader.
   */
  class RichDataFrameReader(dfReader: DataFrameReader) {

    /**
     * Reads data from kafka.
     *
     * @param spark           SparkSession instance.
     * @param bootstrapServer Bootstrap server url.
     * @param topicName       Topic name.
     * @param startingOffsets Starting offsets value.
     * @param endingOffsets   Ending offsets value.
     * @return
     */
    def fromKafka(spark: SparkSession,
                  bootstrapServer: String,
                  topicName: String,
                  startingOffsets: String,
                  endingOffsets: String): Dataset[String] = {
      val kafkaMessage = dfReader
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", topicName)
        .option("startingOffsets", startingOffsets)
        .option("endingOffsets", endingOffsets)
        .load()

      // extract the value of the message
      kafkaMessage
        .selectExpr("CAST(value AS String)")
        .as[String]
    }
  }

}
