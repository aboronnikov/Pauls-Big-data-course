package com.epam.sparkproducer.api

import java.util.concurrent.{ForkJoinPool, ThreadLocalRandom}
import java.util.stream._

import com.epam.sparkproducer.misc.LambdaHelpers.{funToCallable, funToConsumer}
import com.epam.sparkproducer.program.MessageCallback
import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

/**
 * Utility class.
 * Provides functionality to write into kafka topic asynchronously.
 */
object KafkaProducerUtils {

  /**
   * Class for serializing csv data.
   *
   * @param hour hours.
   * @param line line.
   */
  final case class CsvRecordDto(hour: Int, line: String)

  /**
   * Some type definitions, to shorten our lines of code.
   */
  private type StringProducer = ThreadLocal[_ <: Producer[String, String]]
  private type StringRecord = ProducerRecord[String, String]

  /**
   * Simply extracts the key from a line.
   * A key from a line in the form of "a,..." is "a",
   * which is basically everything that goes before the first comma.
   *
   * @param line a line from the csv file.
   * @return Key from a line.
   */
  private def extractKeyFromLine(line: String): String = {
    val startIndex = 0
    val commaIndex = line.indexOf(",")
    line.substring(startIndex, commaIndex)
  }

  /**
   * Gson instance for this class.
   */
  private val Gson = new Gson()

  /**
   * Writes a file stream into kafka in parallel.
   *
   * @param nThreads   number of threads to use.
   * @param producer   thread local producer instance.
   * @param lineStream the stream of lines.
   * @param topic      the topic to write to.
   */
  def writeToKafkaInParallel(nThreads: Int,
                             producer: StringProducer,
                             lineStream: Stream[String],
                             topic: String): Unit = {
    val pool = new ForkJoinPool(nThreads)

    val numberOfHours = 24

    val sendLineToKafka = funToConsumer((line: String) => {
      val key = extractKeyFromLine(line)
      val random = ThreadLocalRandom.current() // get this thread's Random
      val csvRecord = Gson.toJson(CsvRecordDto(hour = random.nextInt(numberOfHours), line))
      val record = new StringRecord(topic, key, csvRecord)
      val prod = producer.get()
      prod.send(record, MessageCallback)
    })

    val task = funToCallable(() => {
      lineStream
        .parallel()
        .forEach(sendLineToKafka)
    })

    pool.submit(task).get()
  }
}
