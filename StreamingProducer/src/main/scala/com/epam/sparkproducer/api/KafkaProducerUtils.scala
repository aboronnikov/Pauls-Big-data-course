package com.epam.sparkproducer.api

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function.Supplier

import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

/**
 * Utility class.
 * Provides functionality to write into kafka topic asynchronously.
 */
object KafkaProducerUtils {
  /**
   * Simply extracts the key from a line.
   * A key from a line in the form of "a,..." is "a",
   * which is basically everything that goes before the first comma.
   *
   * @param line a line from the csv file.
   * @return Key from a line.
   */
  private def extractKeyFromLine(line: String): String = {
    line.substring(0, line.indexOf(","))
  }

  /**
   * Sends a line into kafka topic, using a kafka producer.
   *
   * @param kafkaProducer kafka producer that sends data.
   * @param line          a line from csv file.
   * @param topic         the kafka topic that we are subscribed to.
   */
  private def send(kafkaProducer: Producer[String, String],
                   line: String,
                   topic: String): Unit = {
    val key = extractKeyFromLine(line)
    val record = new ProducerRecord[String, String](topic, key, line)
    kafkaProducer.send(record, MessageCallback)
  }

  /**
   * A method that asynchrnously writes to a kafka topic, using the specified kafka producer.
   *
   * @param reader        source file reader.
   * @param kafkaProducer kafka producer that sends data into a kafka topic.
   * @param executor      executor with the needed number of threads.
   * @param topic         the topic that we are subscribed to.
   * @return returns a future, so that we can wait on it for completion.
   */
  def writeToKafkaAsync(reader: Iterator[String],
                        kafkaProducer: Producer[String, String],
                        executor: ExecutorService,
                        topic: String): CompletableFuture[Void] = {
    CompletableFuture.supplyAsync(new Supplier[Void] {
      override def get(): Void = {
        var line: String = null
        reader.synchronized {
          line = reader.next()
        }
        while (line != null) {
          send(kafkaProducer, line, topic)
          reader.synchronized {
            line = reader.next()
          }
        }
        null
      }
    }, executor)
  }
}
