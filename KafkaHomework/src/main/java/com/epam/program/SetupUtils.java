package com.epam.program;

import com.epam.producer.TwitterProducer;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Utils for setting things up.
 */
public final class SetupUtils {

    /**
     * Private constructor to prevent instantiation.
     */
    private SetupUtils() {

    }

    /**
     * Sets up producer config.
     *
     * @param bootstrapUrl bootstrap server url.
     * @return config properties.
     */
    private static Properties setupProducerConfig(String bootstrapUrl) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.epam.producer.HashtagsAndLocationPartitioner");
        return properties;
    }

    /**
     * Sets up the twitter client.
     *
     * @param keywords  keywords to look for in tweets.
     * @param passStore passStore that keeps keys.
     * @param queue     a queue that the client will be writing messages into.
     * @return client instance.
     */
    private static Client setupClient(List<String> keywords,
                                      String passStore,
                                      BlockingQueue<String> queue) {
        StoreReader reader = PalDB.createReader(new File(passStore));

        String consumerApiKey = reader.get("consumer api key");
        String consumerSecretKey = reader.get("consumer secret key");
        String accessTokenKey = reader.getString("access token key");
        String accessTokenSecretKey = reader.getString("access token secret key");

        reader.close();

        Authentication auth = new OAuth1(consumerApiKey, consumerSecretKey, accessTokenKey, accessTokenSecretKey);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(keywords);

        return new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

    /**
     * Sets up a TwitterProducer to populate Kafka with tweets.
     *
     * @param passStore       path to the pathstore.
     * @param keywords        hashtags to use as filter.
     * @param bootstrapUrl    bootstrap url.
     * @param topic           topic name.
     * @return returns an appropriate TwitterProducer for the task.
     */
    public static TwitterProducer setupTwitterProducer(String passStore, List<String> keywords, String bootstrapUrl, String topic) {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = SetupUtils.setupClient(keywords, passStore, queue);
        client.connect();

        Properties properties = SetupUtils.setupProducerConfig(bootstrapUrl);
        Producer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        return new TwitterProducer(queue, topic, kafkaProducer, client);
    }
}
