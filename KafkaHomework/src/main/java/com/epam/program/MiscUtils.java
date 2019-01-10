package com.epam.program;

import com.jayway.jsonpath.JsonPath;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * Miscellaneous utils. All are different to deserve their own library.
 */
public final class MiscUtils {

    /**
     * Default logger.
     */
    private static final Logger LOG = Logger.getLogger(MiscUtils.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private MiscUtils() {

    }

    /**
     * Since BlockingQueue's stream() method does not block, we have to use this approach:
     * https://stackoverflow.com/questions/23462209/stream-api-and-queues-subscribe-to-blockingqueue-stream-style
     *
     * @param queue blocking queue with tweets.
     * @return blocking stream of tweets.
     */
    public static Stream<String> getQueueStream(@Nonnull BlockingQueue<String> queue) {
        return Stream.generate(() -> {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                LOG.info("An interrupt was called.", e);
                Thread.currentThread().interrupt();
                return null;
            }
        });
    }

    /**
     * Parser utility method, extracts hashtags and location from a json string.
     *
     * @param value json string.
     * @return hashtags and location.
     */
    public static String makeKey(@Nonnull String value) {
        List<String> hashTags = extractHashtags(value);
        String location = extractLocation(value);

        String hashTagString = String.join(",", hashTags);
        return hashTagString + ";" + location;
    }

    /**
     * Extracts hashtags from a json string.
     *
     * @param value json string.
     * @return list of hashtags from this tweet.
     */
    public static List<String> extractHashtags(String value) {
        return JsonPath.parse(value).read("$.entities.hashtags[*].text");
    }

    /**
     * Extracts location from a json string.
     *
     * @param value json string.
     * @return location value.
     */
    public static @Nonnull
    String extractLocation(String value) {
        String result = JsonPath
                .parse(value)
                .read("$.user.location");
        return result == null ? "" : result;
    }
}
