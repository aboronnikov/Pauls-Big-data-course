package com.epam.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Partitioner class. Key is hashtag + some other field (A String representation).
 */
public class HashtagsAndLocationPartitioner implements Partitioner {

    /**
     * Assigns partition to a specific record.
     *
     * @param topic      kafka topic.
     * @param key        record key.
     * @param keyBytes   record key as bytes.
     * @param value      record value.
     * @param valueBytes record value as bytes.
     * @param cluster    a representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
     * @return partition number that this record belongs to.
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfo = cluster.availablePartitionsForTopic(topic);
        int numberOfPartitions = partitionInfo.size();
        // non-negative hashCode of a String object mod number of partitions
        return Math.abs(key.hashCode() % numberOfPartitions);
    }

    /**
     * Does cleanup when partitioner is closed.
     */
    public void close() {
        // Intentionally left blank.
    }

    /**
     * Configures this class with the given key-value pairs.
     *
     * @param map config dictionary.
     */
    public void configure(Map<String, ?> map) {
        // Intentionally left blank.
    }
}
