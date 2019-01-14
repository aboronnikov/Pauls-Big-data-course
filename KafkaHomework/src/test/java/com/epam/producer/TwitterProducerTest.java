package com.epam.producer;

import com.epam.mocks.TestData;
import com.epam.mocks.TestProducer;
import com.twitter.hbc.core.Client;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests the unbounded producer.
 */
public class TwitterProducerTest {

    /**
     * Sets the prelims.
     */
    @Before
    public void setUp() {
        //set up queue
        queue = new LinkedBlockingQueue<>();
        queue.add(TestData.TEST1);
        queue.add(TestData.TEST2);
        queue.add(TestData.TEST3);

        // set up cluster
        cluster = Mockito.mock(Cluster.class, Mockito.RETURNS_DEEP_STUBS);
        List<PartitionInfo> fakeInfos = Arrays.asList(null, null);
        Mockito.when(cluster.availablePartitionsForTopic(Mockito.any(String.class))).thenReturn(fakeInfos);

        // set up producer
        producer = new TestProducer(cluster);
        client = Mockito.mock(Client.class);
    }

    /**
     * Necessary objects.
     */
    private BlockingQueue<String> queue;
    private Cluster cluster;
    private TestProducer producer;
    private Client client;
    private String topic = "hadoop";

    /**
     * Tests the main method.
     */
    @Test
    public void run() {
        try (TwitterProducer twitterProducer = new TwitterProducer(queue, topic, producer, client)) {
            CompletableFuture.runAsync(twitterProducer::run);

            // This loop is here to wait until the producer receives our 3 test messages.
            // If we don't wait long enough, the results won't match with what we expect.
            while (producer.getEntryCount() < 3) {
            }

            List<String> partitionZeroData = producer.getPartitionData(0);
            List<String> partitionOneData = producer.getPartitionData(1);

            List<String> expectedPartitionZeroData = Collections.singletonList(TestData.TEST2);
            List<String> expectedPartitionOneData = Arrays.asList(TestData.TEST1, TestData.TEST3);

            Assert.assertEquals(expectedPartitionZeroData, partitionZeroData);
            Assert.assertEquals(expectedPartitionOneData, partitionOneData);
        }
    }
}