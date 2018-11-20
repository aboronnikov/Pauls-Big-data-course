package com.epam.sparkconsumer.program

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

      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", url)
        .option("subscribe", topic)
        .load()
        .write
        .format(fileFormat)
        .save(filePath)

      spark.stop()
    } else if (cmdLine.hasOption(CmdUtils.Help)) {
      CmdUtils.printHelp()
    } else if (Log.isInfoEnabled) {
      Log.info("Ask help for how to use this.")
    }
  }
}
