package com.epam.sparkconsumer.program

import org.apache.commons.lang3.BooleanUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * A simple consumer that reads from our kafka topic, using spark streaming.
 */
object Consumer {

  /**
   * Default logger for this program.
   */
  private val Log = Logger.getLogger(Consumer.getClass)

  def saveFileToHdfs(spark: SparkSession,
                     doBatch: Boolean,
                     url: String,
                     topic: String,
                     fileFormat: String,
                     filePath: String): Unit = {
    if (doBatch) {
      spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", url)
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS String)")
        .write
        .format(fileFormat)
        .save(filePath)
    } else {
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", url)
        .option("subscribe", topic)
        .load()
        .select("value")
        .writeStream
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()
        .awaitTermination()
//        .toDF()
//        .writeStream
//        .option("checkpointLocation", "/tmp/checkpoint")
//        //.format(fileFormat)
//        .outputMode("append")
//
//        .start(filePath)
//        .awaitTermination()
    }
  }

  /**
   * The entry point.
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val cmdLine = CmdUtils.parserArgs(args)

    if (CmdUtils.areArgumentsGood(cmdLine)) {
      val spark = SparkSession.builder()
        .appName("Consumer")
        .getOrCreate()

      val topic = cmdLine.getOptionValue(CmdUtils.Topic)
      val url = cmdLine.getOptionValue(CmdUtils.Url)
      val filePath = cmdLine.getOptionValue(CmdUtils.FilePath)
      val fileFormat = cmdLine.getOptionValue(CmdUtils.FileFormat)
      val doBatchStr = cmdLine.getOptionValue(CmdUtils.DoBatch)
      val doBatch = BooleanUtils.toBoolean(doBatchStr)

      saveFileToHdfs(spark, doBatch, url, topic, fileFormat, filePath)

      spark.stop()
    } else if (cmdLine.hasOption(CmdUtils.Help)) {
      CmdUtils.printHelp()
    } else if (Log.isInfoEnabled) {
      Log.info("Ask help for how to use this.")
    }
  }
}
