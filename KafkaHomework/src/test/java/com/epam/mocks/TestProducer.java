package com.epam.mocks;

import com.epam.producer.HashtagsAndLocationPartitioner;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This a stub for the producer class, only the necessary methods are implemented.
 */
public class TestProducer implements Producer<String, String> {

    /**
     * Map that keeps track of partitions.
     */
    private Map<Integer, List<String>> partitions = new HashMap<>();

    /**
     * Custom partitioner.
     */
    private Partitioner partitioner = new HashtagsAndLocationPartitioner();

    /**
     * Cluster object to be used by partitioner.
     */
    private Cluster cluster;

    /**
     * Doesn't work without being atomic, because of CPU caches.
     */
    private AtomicInteger entryCount = new AtomicInteger(0);

    /**
     * Gets data from a particular partition.
     *
     * @param partitionNum number of the partition in question.
     * @return data for this partition.
     */
    public List<String> getPartitionData(int partitionNum) {
        return partitions.get(partitionNum);
    }

    /**
     * Constructor for this class.
     *
     * @param cluster cluster to be used by partitioner.
     */
    public TestProducer(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Gets the number of processed entries, this is necessary for the unbounded producer.
     *
     * @return number of processed entries.
     */
    public int getEntryCount() {
        return entryCount.get();
    }

    /**
     * Emulates sending a message to kafka.
     *
     * @param producerRecord message record.
     * @return null in this case, because I don't care about the metadata.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {

        int partitionNum = partitioner.partition(producerRecord.topic(), producerRecord.key(), null, producerRecord.value(), null, cluster);

        if (partitions.containsKey(partitionNum)) {
            partitions.get(partitionNum).add(producerRecord.value());
        } else {
            List<String> records = new ArrayList<>();
            records.add(producerRecord.value());
            partitions.put(partitionNum, records);
        }

        entryCount.incrementAndGet();

        return null;
    }

    @Override
    public void initTransactions() {
        // Intentionally left blank
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        // Intentionally left blank
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
        // Intentionally left blank
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        // Intentionally left blank
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        // Intentionally left blank
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
        // Intentionally left blank
        return null;
    }

    @Override
    public void flush() {
        // Intentionally left blank
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        // Intentionally left blank
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // Intentionally left blank
        return null;
    }

    @Override
    public void close() {
        // Intentionally left blank
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        // Intentionally left blank
    }
}
