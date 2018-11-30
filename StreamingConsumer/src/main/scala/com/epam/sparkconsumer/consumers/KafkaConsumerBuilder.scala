package com.epam.sparkconsumer.consumers

/**
 * Builder for making different kinds of consumers.
 * In order for the consumer to work properly, you need to provide the following values:
 *
 * 1) spark
 * 2) doBatch
 * 3) url
 * 4) fileFormat
 * 5) filePath
 * 6) topic
 */
class KafkaConsumerBuilder {

  /**
   * Fields, necessary to instantiate an instance of a consumer.
   */
  private var doBatch: Boolean = _
  private var url: String = _
  private var fileFormat: String = _
  private var filePath: String = _
  private var topic: String = _

  /**
   * Used to specify the kind of consumer.
   *
   * @param doBatch is this a batching consumer?
   * @return ConsumerBuilder.
   */
  def withBatch(doBatch: Boolean): KafkaConsumerBuilder = {
    this.doBatch = doBatch
    this
  }

  /**
   * Used to specify the kafka url for the consumer.
   *
   * @param url kafka url.
   * @return ConsumerBuilder.
   */
  def withUrl(url: String): KafkaConsumerBuilder = {
    this.url = url
    this
  }

  /**
   * Used to specify the file format.
   *
   * @param fileFormat file format.
   * @return ConsumerBuilder.
   */
  def withFileFormat(fileFormat: String): KafkaConsumerBuilder = {
    this.fileFormat = fileFormat
    this
  }

  /**
   * Used to specify the path to the file (index in case of elastic search) to write to.
   *
   * @param filePath file path.
   * @return ConsumerBuilder.
   */
  def withFilePath(filePath: String): KafkaConsumerBuilder = {
    this.filePath = filePath
    this
  }

  /**
   * Used to specify the kafka topic to read from.
   *
   * @param topic kafka topic to read from.
   * @return ConsumerBuilder.
   */
  def withTopic(topic: String): KafkaConsumerBuilder = {
    this.topic = topic
    this
  }

  /**
   * Used to build the necessary instance of a consumer.
   *
   * @return Consumer instance.
   */
  def build(): KafkaConsumer = {
    if (doBatch) {
      new KafkaBatchingConsumer(url, topic, fileFormat, filePath)
    } else {
      new KafkaStreamingConsumer(url, topic, fileFormat, filePath)
    }
  }
}
