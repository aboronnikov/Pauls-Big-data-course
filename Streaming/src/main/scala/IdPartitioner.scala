import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class IdPartitioner extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val maxPartitionNumber = cluster.availablePartitionsForTopic(topic).size()
    val idString = key.asInstanceOf[String]
    val id = Integer.parseInt(idString)
    id % maxPartitionNumber
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
