import java.lang.Boolean
import java.util
import java.util.concurrent._
import java.util.{Collections, Map}

import com.epam.sparkproducer.api.KafkaProducerUtils
import com.epam.sparkproducer.api.KafkaProducerUtils.CsvRecordDto
import com.epam.sparkproducer.misc.LambdaHelpers.{funToConsumer, funToFunction, funToSupplier}
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Producer, _}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import org.junit.{Assert, Before, Test}
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
    val threadSafeSet = Collections.newSetFromMap(new ConcurrentHashMap[String, Boolean]())
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
   * Type definitions to reduce code complexity.
   */
  private type StringProducer = Producer[String, String]
  private type ProducerMap = Map[Thread, StringProducer]
  private type ThreadLocalProducer = ThreadLocal[StringProducer]

  /**
   * Variables necessary to run tests.
   */
  private var threadLocalProducer: ThreadLocalProducer = _
  private val threadLocalMap = new ConcurrentHashMap[Thread, StringProducer]()
  private val gson = new Gson()

  /**
   * Sets up all the necessary variables.
   */
  @Before
  def setUp(): Unit = {
    val computeProducerIfAbsent = funToSupplier(
      () => threadLocalMap.computeIfAbsent(
        Thread.currentThread(), funToFunction((thread: Thread) => new MockProducer)
      )
    )

    threadLocalProducer = ThreadLocal.withInitial(computeProducerIfAbsent)
  }

  /**
   * The test for writeToKafkaInParallel.
   */
  @Test
  def writeToKafkaInParallelTest(): Unit = {
    val expectedList = util.Arrays.asList("key,a", "key,b", "key,c", "key,d", "key,e", "key,f")
    val lineStream = expectedList.stream()

    KafkaProducerUtils.writeToKafkaInParallel(
      nThreads = 6,
      topic = "test",
      producer = threadLocalProducer,
      lineStream = lineStream
    )

    val resultSet = MockProducer.threadSafeSet
    Assert.assertEquals(expectedList.size(), resultSet.size())

    resultSet
      .stream()
      .forEach(
        funToConsumer((line: String) => {
          val record = gson.fromJson(line, classOf[CsvRecordDto])
          Assert.assertTrue(expectedList.contains(record.line))
        })
      )
  }
}
