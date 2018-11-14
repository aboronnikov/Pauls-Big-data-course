import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.Logger

import scala.io.Source

object Producer {
  //private val Log = Logger.getLogger(MessageCallback.getClass)

  def main(args: Array[String]): Unit = {
    val url = args(2)
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "IdPartitioner")
    ProducerConfig.
    var kafkaProducer: KafkaProducer[String, String] = null
    println("created config")
    try {
      kafkaProducer = new KafkaProducer[String, String](props)
      println("created kafka producer")
      //Log.info("created kafka producer")
      val fileName = args(0)
      val topic = args(1)
      val fileSource = Source.fromFile(fileName)
        .getLines()
        .drop(1)
        .foreach(
          line => {
           // Log.info("LINE")
            println("LINE: " + line)
            val key = line.substring(0, line.indexOf(','))
            val record = new ProducerRecord[String, String](topic, "123", line)
            kafkaProducer.send(record, MessageCallback)
          }
        )
    } catch {
      case e: Exception => //Log.error("THE ERROR:" + e.getMessage)
    } finally {
      if (kafkaProducer != null) {
        kafkaProducer.close()
      }
    }
  }
}
