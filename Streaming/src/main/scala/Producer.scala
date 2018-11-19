import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties
import java.util.concurrent.{CompletableFuture, Executors}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.Logger

import scala.io.Source

object Producer {
  //private val Log = Logger.getLogger(MessageCallback.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length == 4) {
      val url = args(1)
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "600000")
      props.put(ProducerConfig.BATCH_SIZE_CONFIG,"100000")
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "66554432")

      val kafkaProducer = new KafkaProducer[String, String](props)
      val nThreads = Integer.parseInt(args(2))
      val topic = args(0)
      val futures = new Array[CompletableFuture[Unit]](nThreads)
      val executor = Executors.newFixedThreadPool(nThreads)
      val bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(args(3))))
      for (i <- 0 until nThreads) {
        futures(i) = CompletableFuture.supplyAsync(() => {
          var line: String = null
          bufferedReader.synchronized {
            line = bufferedReader.readLine()
          }
          while (line != null) {
            val key = line.substring(0, line.indexOf(","))
            val record = new ProducerRecord[String, String](topic, key, "Thread num: " + i + " ==> " + line)
            kafkaProducer.send(record, MessageCallback)
            bufferedReader.synchronized {
              line = bufferedReader.readLine()
            }
          }

        }, executor)
        futures(i).exceptionally(e => {
          println(e.getMessage)
        })
      }
      CompletableFuture.allOf(futures: _*)
      //kafkaProducer.close()
    } else {
      println("First arg is url, second arg is nThreads, third arg is topic")
    }
  }
}
