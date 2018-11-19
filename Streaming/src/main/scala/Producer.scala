import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.Logger
import resource.managed

/**
  * Producer class.
  * This is the main class of this program.
  */
object Producer {

  /**
    * Default logger.
    */
  private val Log = Logger.getLogger(MessageCallback.getClass)

  /**
    * This method builds properties for our kafka producers.
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
    * @param nThreads number of threads, specified on command line.
    * @param topic topic name.
    * @param bufferedReader reader for the file we're reading from.
    * @param url topic's url.
    * @return Array of completable futures of our actions.
    */
  private def writeIntoKafkaWithNThreads(nThreads: Int,
                                         topic: String,
                                         bufferedReader: BufferedReader,
                                         url: String): Array[CompletableFuture[Unit]] = {
    val futures = new Array[CompletableFuture[Unit]](nThreads)
    val executor = Executors.newFixedThreadPool(nThreads)
    for {
      i <- 0 until nThreads
      val properties = buildProperties(url)
      kafkaProducer <- managed(new KafkaProducer[String, String](properties))
    } {
      futures(i) = KafkaProducerUtils.writeToKafkaAsync(bufferedReader, kafkaProducer, executor, topic)
      futures(i).exceptionally(e => if (Log.isInfoEnabled) {
        Log.info(e.getMessage)
      })
    }
    futures
  }

  /**
    * Entry point of our program.
    * @param args cmd args.
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 4) {
      val topic = args(0)
      val url = args(1)
      val nThreads = Integer.parseInt(args(2))
      val filePath = args(3)
      for {
        bufferedReader <- managed(new BufferedReader(new InputStreamReader(new FileInputStream(filePath))))
      } {
        val futures = writeIntoKafkaWithNThreads(nThreads, topic, bufferedReader, url)
        CompletableFuture.allOf(futures: _*).join()
      }
    } else if (Log.isInfoEnabled) {
      Log.info("First arg is url, second arg is nThreads, third arg is topic")
    }
  }
}
