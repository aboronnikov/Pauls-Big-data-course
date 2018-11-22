package com.epam.sparkproducer.program

import java.util.Properties
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}

import com.epam.sparkproducer.api.KafkaProducerUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.log4j.Logger
import resource.managed

import scala.compat.java8.FunctionConverters._
import scala.io.Source

/**
 * Producer class.
 * This is the main class of this program.
 */
object Producer {

  /**
   * Default logger.
   */
  private val Log = Logger.getLogger(Producer.getClass)

  /**
   * This method builds properties for our kafka producers.
   *
   * @param url topic's url.
   * @return Properties object.
   */
  private def buildProperties(url: String): Properties = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000000")
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "0")
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "66554432")
    properties
  }

  /**
   * Properly closes kafka producer.
   *
   * @param kafkaProducer kafka producer that writes messages into our topic.
   */
  private def closeProducer(kafkaProducer: KafkaProducer[String, String]): Unit = {
    try {
      // can't use try with resources here, because it's Closeable but not AutoCloseable
      kafkaProducer.close()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        Log.info("Failed to run a producer.")
        Log.info(e.getMessage)
    }
  }

  /**
   * Sets up a kafka producer.
   * It first creates the producer then uses it to write in to a kafka topic asynchronously.
   *
   * @param threadId   the id of this producers thread.
   * @param futures    the array of futures.
   * @param properties properties of our producer.
   * @param reader     file reader.
   * @param executor   executor, so that we don't use only the default number of threads.
   * @param topic      name of the topic.
   */
  private def setUpKafkaProducer(threadId: Int,
                                 futures: Array[CompletableFuture[Unit]],
                                 properties: Properties,
                                 reader: Iterator[String],
                                 executor: ExecutorService,
                                 topic: String): Unit = {

    val kafkaProducer = new KafkaProducer[String, String](properties)

    futures(threadId) = KafkaProducerUtils.writeToKafkaAsync(reader, kafkaProducer, executor, topic)

    futures(threadId).whenComplete(((result: Unit, e: Throwable) =>
      if (e != null && Log.isInfoEnabled) {
        Log.info(e.getMessage)
      }).asJava)

    closeProducer(kafkaProducer)
  }

  /**
   * This method creates nThreads threads and asynchronously writes to a kafka topic, using nThreads producers.
   *
   * @param nThreads number of threads, specified on command line.
   * @param topic    topic name.
   * @param reader   reader for the file we're reading from.
   * @param url      topic's url.
   * @return Array of completable futures of our actions.
   */
  private def writeIntoKafkaWithNThreads(nThreads: Int,
                                         topic: String,
                                         reader: Iterator[String],
                                         url: String): Array[CompletableFuture[Unit]] = {
    val futures = new Array[CompletableFuture[Unit]](nThreads)
    val executor = Executors.newFixedThreadPool(nThreads)
    for {
      i <- 0 until nThreads
    } {
      val properties = buildProperties(url)
      setUpKafkaProducer(i, futures, properties, reader, executor, topic)
    }
    futures
  }

  /**
   * Entry point of our program.
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val cmdLine = CmdUtils.parserArgs(args)
    if (CmdUtils.areArgumentsGood(cmdLine)) {
      val topic = cmdLine.getOptionValue(CmdUtils.Topic)
      val url = cmdLine.getOptionValue(CmdUtils.Url)
      val nThreadsStr = cmdLine.getOptionValue(CmdUtils.NThreads)
      val nThreads = Integer.parseInt(nThreadsStr)
      val filePath = cmdLine.getOptionValue(CmdUtils.FilePath)

      for {
        reader <- managed(Source.fromFile(filePath))
      } {
        val futures = writeIntoKafkaWithNThreads(nThreads, topic, reader.getLines(), url)
        CompletableFuture.allOf(futures: _*).join()
      }
    } else if (cmdLine.hasOption(CmdUtils.Help)) {
      CmdUtils.printHelp()
    } else if (Log.isInfoEnabled) {
      Log.info("Ask help for how to use this.")
    }
  }
}
