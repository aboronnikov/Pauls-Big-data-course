package com.epam.sparkproducer.program

import java.nio.file.{Files, Paths}
import java.util.concurrent._
import java.util.{Map, Properties}

import com.epam.sparkproducer.api.KafkaProducerUtils
import com.epam.sparkproducer.misc.LambdaHelpers.{funToConsumer, funToFunction, funToSupplier}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import resource.managed

/**
 * Producer class.
 * This is the main class of this program.
 */
object Producer {

  /**
   * Some type definitions, to shorten our lines of code.
   */
  private type StringProducer = KafkaProducer[String, String]
  private type ProducerMap = Map[Thread, StringProducer]
  private type ThreadLocalProducer = ThreadLocal[StringProducer]

  /**
   * This method builds properties for our kafka producers.
   *
   * @param url topic's url.
   * @return Properties object.
   */
  private def buildProperties(url: String): Properties = {
    val stringSerializerStr = "org.apache.kafka.common.serialization.StringSerializer"
    val requestTimeoutMsValueStr = "10000000"
    val batchSizeValueStr = "0"
    val bufferMemoryValueStr = "66554432"

    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializerStr)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializerStr)
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMsValueStr)
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeValueStr)
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemoryValueStr)
    properties
  }

  /**
   * Builds a thread local producer instance.
   *
   * @return ThreadLocal[KafkaProducer].
   */
  private def buildThreadLocalProducer(url: String,
                                       threadLocalMap: ProducerMap): ThreadLocalProducer = {
    val properties = buildProperties(url)
    val computeProducerIfAbsent = funToSupplier(() => {
      threadLocalMap.computeIfAbsent(
        Thread.currentThread(), funToFunction((thread: Thread) => new StringProducer(properties))
      )
    }
    )
    val threadLocalProducer = ThreadLocal.withInitial(computeProducerIfAbsent)
    threadLocalProducer
  }

  /**
   * Closes thread local producers.
   *
   * @param threadLocalMap map from thread to producer.
   */
  private def closeThreadLocalResources(threadLocalMap: ProducerMap): Unit = {
    val closeProducer = funToConsumer((producer: StringProducer) => producer.close())
    threadLocalMap
      .values()
      .forEach(closeProducer)
  }

  /**
   * The entry point of the program.
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val cmdLine = CmdUtils.parserArgs(args)
    CmdUtils.printHelpIfNeeded(cmdLine)
    CmdUtils.checkArguments(cmdLine) // I don't catch the exception on purpose.

    val topic = cmdLine.getOptionValue(CmdUtils.Topic)
    val url = cmdLine.getOptionValue(CmdUtils.Url)
    val nThreadsStr = cmdLine.getOptionValue(CmdUtils.NThreads)
    val nThreads = Integer.parseInt(nThreadsStr)
    val filePath = cmdLine.getOptionValue(CmdUtils.FilePath)

    for {
      lineStream <- managed(Files.lines(Paths.get(filePath)))
    } {
      val threadLocalMap = new ConcurrentHashMap[Thread, StringProducer]()
      val threadLocalProducer = buildThreadLocalProducer(url, threadLocalMap)
      KafkaProducerUtils.writeToKafkaInParallel(nThreads, threadLocalProducer, lineStream, topic)
      closeThreadLocalResources(threadLocalMap)
    }
  }
}
