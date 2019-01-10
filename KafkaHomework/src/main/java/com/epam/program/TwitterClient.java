package com.epam.program;

import com.epam.producer.TwitterProducer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * A twitter client class, consumes messages from twitter and writes them into a kafka topic.
 */
public class TwitterClient {

    /**
     * Default logger.
     */
    private static final Logger LOG = Logger.getLogger(TwitterClient.class);

    /**
     * Program's entry point.
     *
     * @param args necessary cmd args.
     */
    public static void main(String[] args) {
        try {
            CommandLine commandLine = CmdUtils.parseArgs(args);
            proceedAccordingToArgs(commandLine);
        } catch (ParseException | NumberFormatException e) {
            LOG.error("Bad cmd arguments", e);
        }
    }

    /**
     * Either prints help or starts the program.
     *
     * @param commandLine parse cmd args.
     * @throws ParseException in case parsing fails.
     */
    private static void proceedAccordingToArgs(CommandLine commandLine) throws ParseException {
        if (commandLine.hasOption(CmdUtils.HELP)) {
            CmdUtils.printHelp();
        } else {
            String topic = CmdUtils.getTopic(commandLine);
            String url = CmdUtils.getBootstrapUrl(commandLine);
            String passStore = CmdUtils.getPassStore(commandLine);
            List<String> keywords = CmdUtils.getKeywords(commandLine);
            int howManyMessages = CmdUtils.getHowManyMessages(commandLine);

            writeToKafkaFromTwitter(topic, url, passStore, keywords, howManyMessages);
        }
    }

    /**
     * Writes to kafka using bounded or unbounded producer.
     *
     * @param topic           topic to write into.
     * @param bootstrapUrl    bootstrap server url.
     * @param passStore       passStore that keeps keys.
     * @param keywords        keywords to look for in tweets.
     * @param howManyMessages how many messages to consumer, -1 for unbounded.
     */
    private static void writeToKafkaFromTwitter(String topic,
                                                String bootstrapUrl,
                                                String passStore,
                                                List<String> keywords,
                                                int howManyMessages) {
        try (TwitterProducer twitterProducer =
                     SetupUtils.setupTwitterProducer(howManyMessages, passStore, keywords, bootstrapUrl, topic)) {
            twitterProducer.run();
        }
    }
}