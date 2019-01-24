package com.epam.extensions

import org.apache.spark.sql.{Dataset, SparkSession}
import scala.language.implicitConversions

/**
  * Methods I wish I had in spark.
  */
object SparkExtensions {
  /**
    * Pimp my library pattern.
    *
    * @param spark this spark session, on which the methods will be invoked.
    * @return enriched spark.
    */
  implicit def richSpark(spark: SparkSession): RichSpark =
    new RichSpark(spark)

  /**
    * Pimp my library pattern.
    *
    * @param spark this spark session, on which the methods will be invoked.
    */
  class RichSpark(spark: SparkSession) {
    /**
      * Reads a data stream from kafka.
      *
      * @param bootstrapServer bootstrap server associated with out topic.
      * @param inputTopic      topic to be read from.
      * @param startingOffsets starting offsets inside the topic.
      * @return a dataset with strings read from the kafka topic provided.
      */
    def readStreamFromKafka(bootstrapServer: String,
                            inputTopic: String,
                            startingOffsets: String): Dataset[String] = {
      import spark.implicits._
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", inputTopic)
        .option("startingOffsets", startingOffsets)
        .load()

      df.selectExpr("CAST(value AS String)")
        .as[String]
    }
  }

}
