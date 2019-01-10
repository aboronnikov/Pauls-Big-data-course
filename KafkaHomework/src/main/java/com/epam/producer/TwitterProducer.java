package com.epam.producer;

/**
 * Common interface of Unbounded and Bounded TwitterProducers.
 */
public interface TwitterProducer extends AutoCloseable {

    /**
     * Runs this producer, putting twitter messages into kafka.
     */
    void run();

    /**
     * Does cleanup.
     */
    @Override
    void close();
}
