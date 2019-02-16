package com.epam.extensions

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{DataFrame, Row}
import scala.language.implicitConversions

/**
  * Methods I wish I had in DataFrame.
  */
object DataFrameExtensions {
  /**
    * Pimp my library pattern.
    *
    * @param dataFrame this DataFrame. (On which the methods will be invoked).
    * @return dataframe enriched with new methods.
    */
  implicit def richDataFrame(dataFrame: DataFrame): RichDataFrame = new RichDataFrame(dataFrame)

  /**
    * Pimp my library pattern.
    *
    * @param dataFrame this DataFrame. (On which the methods will be invoked).
    */
  class RichDataFrame(dataFrame: DataFrame) {
    /**
      * Method for writing into Kafka.
      *
      * @param checkPointLocation check pointing folder.
      * @param bootstrapServer    bootstrapServer associated with the output topic.
      * @param outputTopic        topic to be written to.
      * @param outputMode         output mode.
      * @return a decorated DataStreamWriter.
      */
    def writeStreamToKafka(checkPointLocation: String,
                           bootstrapServer: String,
                           outputTopic: String, outputMode: OutputMode): DataStreamWriter[Row] = {
      dataFrame.writeStream
        .outputMode(outputMode)
        .format("kafka")
        .option("checkpointLocation", checkPointLocation)
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("topic", outputTopic)
    }
  }

}
