package com.epam.sparkconsumer.consumers

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

/**
 * A consumer that uses streaming.
 *
 * @param url        url.
 * @param topic      topic.
 * @param fileFormat file format.
 * @param filePath   file path.
 */
class KafkaStreamingConsumer(url: String, topic: String, fileFormat: String, filePath: String) extends KafkaConsumer {

  /**
   * Default logger for this class.
   */
  private val log = Logger.getLogger(classOf[KafkaStreamingConsumer])

  /**
   * This is the main function for consuming data.
   *
   * @param spark spark session.
   */
  private def consumeMain(spark: SparkSession): Unit = {
    import spark.implicits.newStringEncoder
    import spark.implicits.newProductEncoder

    spark.readStream
      .format(KafkaConsumer.KafkaStr)
      .option(KafkaConsumer.KafkaBootstrapServersStr, url)
      .option(KafkaConsumer.SubscribeStr, topic)
      .option(KafkaConsumer.StartingOffsetsStr, KafkaConsumer.EarliestStr)
      .load()
      .selectExpr(KafkaConsumer.CastValueAsStringStr)
      .as[String]
      .map((value: String) => KafkaConsumer.Gson.fromJson(value, classOf[CsvRecordDto]))
      .writeStream
      .format(fileFormat)
      .option(KafkaConsumer.PathStr, filePath)
      .option(KafkaConsumer.CheckPointLocationStr, KafkaConsumer.CheckPointLocationPathStr)
      .trigger(Trigger.ProcessingTime(1.second))
      .start()
      .processAllAvailable()
  }

  /**
   * Ingest data into the path specified in the format specified, from the topic specified.
   * This method also times the execution and prints the number of seconds elapsed.
   */
  override def consume(): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .getOrCreate()

    val start = System.nanoTime
    consumeMain(spark)
    val nanoSecsInSecond = 1e9d
    val elapsed = (System.nanoTime - start) / nanoSecsInSecond
    log.info("Elapsed: " + elapsed)

    spark.stop()
  }
}
