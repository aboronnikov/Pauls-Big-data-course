package com.epam.sparkconsumer.consumers

import com.google.gson.Gson

/**
 * Companion object for the Consumer trait.
 * This has common constants and a json converter.
 */
object KafkaConsumer {
  /**
   * Helper string constants, nothing more.
   */
  val KafkaBootstrapServersStr = "kafka.bootstrap.servers"
  val KafkaStr = "kafka"
  val SubscribeStr = "subscribe"
  val CastValueAsStringStr = "CAST(value AS String)"
  val PathStr = "path"
  val CheckPointLocationStr = "checkpointLocation"
  val StartingOffsetsStr = "startingOffsets"
  val EarliestStr = "earliest"

  /**
   * Checkpoint location path on hdfs.
   */
  val CheckPointLocationPathStr = "/tmp/checkpoint"

  /**
   * Gson instance to convert objects to json.
   */
  val Gson = new Gson()
}

/**
 * This is a trait for different kinds of consumers.
 */
trait KafkaConsumer {
  /**
   * Method invoked, to consume data from a kafka topic.
   */
  def consume(): Unit
}
