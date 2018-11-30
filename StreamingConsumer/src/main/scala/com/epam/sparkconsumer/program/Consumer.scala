package com.epam.sparkconsumer.program

import com.epam.sparkconsumer.consumers.KafkaConsumerBuilder
import org.apache.commons.lang3.BooleanUtils
import org.apache.log4j.Logger

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
   * @param doBatch    batch or stream?
   * @param url        kafka topic url
   * @param topic      kafka topic name
   * @param fileFormat file format to save in
   * @param filePath   path to the file on hdfs
   */
  def saveFile(doBatch: Boolean, url: String, topic: String, fileFormat: String, filePath: String): Unit = {
    val consumer = new KafkaConsumerBuilder()
      .withTopic(topic)
      .withUrl(url)
      .withFileFormat(fileFormat)
      .withFilePath(filePath)
      .withBatch(doBatch)
      .build()

    consumer.consume()
  }

  /**
   * The entry point.
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val cmdLine = CmdUtils.parserArgs(args)
    CmdUtils.printHelpIfNeeded(cmdLine)
    CmdUtils.checkArguments(cmdLine) // I don't catch the exception on purpose.

    val topic = cmdLine.getOptionValue(CmdUtils.Topic)
    val url = cmdLine.getOptionValue(CmdUtils.Url)
    val filePath = cmdLine.getOptionValue(CmdUtils.FilePath)
    val fileFormat = cmdLine.getOptionValue(CmdUtils.FileFormat)
    val doBatchStr = cmdLine.getOptionValue(CmdUtils.DoBatch)
    val doBatch = BooleanUtils.toBoolean(doBatchStr)

    saveFile(doBatch, url, topic, fileFormat, filePath)
  }
}
