import java.util
import java.util.Collections
import java.util.concurrent._

import com.epam.sparkproducer.api.KafkaProducerUtils
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Producer, _}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import org.junit.{Assert, Test}
import org.mockito.Mockito
import org.scalatest.junit.JUnitSuite

/**
 * Test class for KafkaProducerUtils.
 */
class KafkaProducerUtilsTest extends JUnitSuite {

  /**
   * Companion object for MockProducer.
   */
  private object MockProducer {

    /**
     * That place where producers send their messages.
     */
    val threadSafeSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())
  }

  /**
   * A mock that is used in place of our kafka producer.
   */
  private class MockProducer extends Producer[String, String] {

    /**
     * The send method writes into our companion object's map.
     * We can then check if all of the records have been sent.
     *
     * @param record   record that we are sending.
     * @param callback callback object for logging.
     * @return
     */
    override def send(record: ProducerRecord[String, String], callback: Callback): Future[RecordMetadata] = {
      MockProducer.threadSafeSet.add(record.value())
      null
    }

    /**
     * All the methods below are useless for this test.
     */
    override def initTransactions(): Unit = ???

    override def beginTransaction(): Unit = ???

    override def sendOffsetsToTransaction(offsets: util.Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): Unit = ???

    override def commitTransaction(): Unit = ???

    override def abortTransaction(): Unit = ???

    override def send(record: ProducerRecord[String, String]): Future[RecordMetadata] = ???

    override def flush(): Unit = ???

    override def partitionsFor(topic: String): util.List[PartitionInfo] = ???

    override def metrics(): util.Map[MetricName, _ <: Metric] = ???

    override def close(): Unit = ???

    override def close(timeout: Long, unit: TimeUnit): Unit = ???
  }

  /**
   * This method tests the writeToKafkaAsync method.
   * We use 2 threads and 2 producers to write several records and at the end we check that everything has
   * arrived successfully.
   */
  @Test
  def writeToKafkaAsyncTest(): Unit = {
    val reader = Mockito.mock(classOf[Iterator[String]])
    Mockito.when(reader.next()).thenReturn("key,a", "key,b", "key,c", "key,d", "key,e", "key,f", null)
    val producer1 = new MockProducer
    val producer2 = new MockProducer
    val executor = Executors.newFixedThreadPool(2)
    val future1 = KafkaProducerUtils.writeToKafkaAsync(reader, producer1, executor, "test")
    val future2 = KafkaProducerUtils.writeToKafkaAsync(reader, producer2, executor, "test")
    CompletableFuture.allOf(future1, future2).join()
    val expected = Array("key,a", "key,b", "key,c", "key,d", "key,e", "key,f")
    expected.foreach(el => {
      Assert.assertTrue(MockProducer.threadSafeSet.contains(el))
    })
  }
}
