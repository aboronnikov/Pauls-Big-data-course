package com.epam.producer;

import com.epam.program.MiscUtils;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Unbounded producer that runs forever.
 */
public class TwitterProducer implements AutoCloseable {

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
    public TwitterProducer(BlockingQueue<String> queue, String topic, Producer<String, String> producer, Client client) {
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
                .filter(this::isFromRussia)
                .forEach(this::sendTweetToKafka);
    }

    /**
     * Checks if this tweet came from Russia.
     *
     * @param tweet received tweet.
     * @return true or false, indicating whether or not this tweet came from Russia.
     */
    private boolean isFromRussia(String tweet) {
        @Nonnull String location = MiscUtils.extractLocation(tweet);
        return location.contains("Russia");
    }

    /**
     * Sends a message with this tweet to kafka.
     *
     * @param tweet received tweet.
     */
    private void sendTweetToKafka(String tweet) {
        String key = MiscUtils.makeKey(tweet);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, tweet);
        producer.send(record);
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
