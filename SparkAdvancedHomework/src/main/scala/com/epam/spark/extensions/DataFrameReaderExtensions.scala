package com.epam.spark.extensions

import org.apache.spark.sql.{DataFrameReader, Dataset, SparkSession}

import scala.language.implicitConversions

object DataFrameReaderExtensions {
  implicit def richDataFrameReader(dfReader: DataFrameReader): RichDataFrameReader = new RichDataFrameReader(dfReader)

  class RichDataFrameReader(dfReader: DataFrameReader) {
    def fromKafka(spark: SparkSession,
                  bootstrapServer: String,
                  topicName: String,
                  startingOffsets: String,
                  endingOffsets: String): Dataset[String] = {
      import spark.implicits._
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
