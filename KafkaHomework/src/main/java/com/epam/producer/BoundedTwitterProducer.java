package com.epam.producer;

import com.epam.program.MiscUtils;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Bounded producer that accepts the specified number of messages.
 */
public class BoundedTwitterProducer implements TwitterProducer {

    /**
     * Default logger.
     */
    private static final Logger LOG = Logger.getLogger(BoundedTwitterProducer.class);

    /**
     * Specifies how many messages are to be processed.
     */
    private int howManyMessages;

    /**
     * Queue, from which we take messages.
     */
    private BlockingQueue<String> queue;

    /**
     * Kafka topic we write into.
     */
    private String topic;

    /**
     * Kafka producer we use to write into the topic.
     */
    private Producer<String, String> producer;

    /**
     * Twitter client we use to populate our queue with messages, kept around to clean up after we're done.
     */
    private Client client;

    /**
     * Constructor of this class.
     */
    public BoundedTwitterProducer(int howManyMessages, BlockingQueue<String> queue, String topic, Producer<String, String> producer, Client client) {
        this.howManyMessages = howManyMessages;
        this.queue = queue;
        this.topic = topic;
        this.producer = producer;
        this.client = client;
    }

    /**
     * Writes messages to a kafka topic.
     * Uses a stream of messages, first it filters out nulls in case of interrupts,
     * second it filters out countries that are not Russia,
     * and then writes this record to an appropriate partition.
     */
    public void run() {
        MiscUtils.getQueueStream(queue)
                .filter(Objects::nonNull)
                .filter(value -> {
                    String location = MiscUtils.extractLocation(value);
                    return MiscUtils.safeContains(location, "Russia");
                })
                .limit(howManyMessages)
                .forEach(value -> {
                    LOG.info(value);

                    String key = MiscUtils.makeKey(value);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    producer.send(record);
                });
    }

    /**
     * Cleanup.
     */
    @Override
    public void close() {
        producer.close();
        client.stop();
    }
}
