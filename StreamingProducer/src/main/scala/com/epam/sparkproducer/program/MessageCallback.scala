package com.epam.sparkproducer.program

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

/**
 * Callback object, that notifies us of exceptional of successful outcome.
 */
object MessageCallback extends Callback {
  /**
   * Default logger.
   */
  private val Log = Logger.getLogger(MessageCallback.getClass)

  /**
   * Method that is executed upon completion of our data transfer.
   *
   * @param metadata  metadata of our transferred record.
   * @param exception exception, if it occurred, otherwise null in its place.
   */
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      Log.error("Message dispatch was not successful", exception)
    } else if (Log.isInfoEnabled) {
      Log.info(
        "Topic : " +
          metadata.topic +
          ", Offset : " +
          metadata.offset +
          ", Partition : " +
          metadata.partition
      )
    }
  }
}
