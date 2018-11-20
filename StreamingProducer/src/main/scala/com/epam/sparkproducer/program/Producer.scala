package com.epam.sparkproducer.program

import java.util.Properties
import java.util.concurrent.{CompletableFuture, Executors}
import java.util.function.BiConsumer

import com.epam.sparkproducer.api.KafkaProducerUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.log4j.Logger
import resource.managed

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
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "600000")
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "100000")
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "66554432")
    properties
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
                                         url: String): Array[CompletableFuture[Void]] = {
    val futures = new Array[CompletableFuture[Void]](nThreads)
    val executor = Executors.newFixedThreadPool(nThreads)
    for {
      i <- 0 until nThreads
    } {
      val properties = buildProperties(url)
      val kafkaProducer = new KafkaProducer[String, String](properties)
      futures(i) = KafkaProducerUtils.writeToKafkaAsync(reader, kafkaProducer, executor, topic)

      /**
       * Can't use a lambda in scala 2.11, so had to resort to using anonymous classes.
       */
      val completionAction = new BiConsumer[Void, Throwable] {
        override def accept(t: Void, u: Throwable): Unit = {
          if (u != null && Log.isInfoEnabled) {
            Log.info(u.getMessage)
          }
          kafkaProducer.close()
        }
      }

      futures(i).whenComplete(completionAction)
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
