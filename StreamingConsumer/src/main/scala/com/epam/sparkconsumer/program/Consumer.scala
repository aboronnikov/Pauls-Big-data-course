package com.epam.sparkconsumer.program

import org.apache.commons.lang3.BooleanUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

/**
 * A simple consumer that reads from our kafka topic, using spark streaming.
 */
object Consumer {

  /**
   * Default logger for this program.
   */
  private val Log = Logger.getLogger(Consumer.getClass)

  /**
   * Saves file to hdfs by either 1) streaming or 2) batching, depending on the doBatch flag.
   *
   * @param spark      currentSparkSession
   * @param doBatch    batch or stream?
   * @param url        kafka topic url
   * @param topic      kafka topic name
   * @param fileFormat file format to save in
   * @param filePath   path to the file on hdfs
   */
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
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS String)")
        .writeStream
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", "/tmp/checkpoint")
        .trigger(Trigger.ProcessingTime(1.second))
        .start()
        .processAllAvailable()
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

      val start = System.nanoTime

      saveFileToHdfs(spark, doBatch, url, topic, fileFormat, filePath)

      val elapsed = (System.nanoTime - start) / 1e9d
      Log.info("Elapsed: " + elapsed)

      spark.stop()
    } else if (cmdLine.hasOption(CmdUtils.Help)) {
      CmdUtils.printHelp()
    } else if (Log.isInfoEnabled) {
      Log.info("Ask help for how to use this.")
    }
  }
}
