package com.epam.program;

import com.google.common.collect.Lists;
import org.apache.commons.cli.*;

import java.util.List;

/**
 * Utility class for handling cmd args.
 */
public final class CmdUtils {

    /**
     * Private ctor to prevent instantiation.
     */
    private CmdUtils() {

    }

    /**
     * Argument constants below
     */
    private static final boolean HAS_ARG = true;
    private static final boolean NO_ARG = false;

    public static final String TOPIC = "topic";
    public static final String URL = "url";
    public static final String PASS_STORE = "passStore";
    public static final String KEYWORDS = "keywords";
    public static final String HELP = "help";
    public static final String HOW_MANY_MESSAGES = "howManyMessages";

    /**
     * Possible command line options for this program.
     */
    private static final Options OPTIONS = new Options()
            .addOption(TOPIC, HAS_ARG, "Topic name. Required.")
            .addOption(URL, HAS_ARG, "Bootstrap server url. Required.")
            .addOption(PASS_STORE, HAS_ARG, "Password store. Required.")
            .addOption(KEYWORDS, HAS_ARG, "Keywords to search. Required.")
            .addOption(HELP, NO_ARG, "Help. Optional.");

    /**
     * Parses cmd arguments into a command line object.
     *
     * @param args cmd args
     * @return Command line object.
     * @throws ParseException exception if something goes wrong.
     */
    public static CommandLine parseArgs(String[] args) throws ParseException {
        PosixParser parser = new PosixParser();
        return parser.parse(OPTIONS, args);
    }

    /**
     * Extracts keywords to look for in tweets as a List.
     *
     * @param commandLine a CommandLine object.
     * @return keywords to look for in tweets as a List.
     * @throws ParseException in case this arg is not present.
     */
    public static List<String> getKeywords(CommandLine commandLine) throws ParseException {
        String keywordsOneLiner = commandLine.getOptionValue(KEYWORDS);
        if (keywordsOneLiner == null) {
            throw new ParseException("-keywords must be provided.");
        }
        String[] splitWords = keywordsOneLiner.split(",");
        return Lists.newArrayList(splitWords);
    }

    /**
     * Extracts the path to passStore.
     *
     * @param commandLine a CommandLine object.
     * @return passStore path.
     * @throws ParseException in case this arg is not present.
     */
    public static String getPassStore(CommandLine commandLine) throws ParseException {
        String passStore = commandLine.getOptionValue(PASS_STORE);
        if (passStore == null) {
            throw new ParseException("-passStore must be provided.");
        }
        return passStore;
    }

    /**
     * Extracts the topic.
     *
     * @param commandLine a CommandLine object.
     * @return topic name.
     * @throws ParseException in case this arg is not present.
     */
    public static String getTopic(CommandLine commandLine) throws ParseException {
        String topic = commandLine.getOptionValue(TOPIC);
        if (topic == null) {
            throw new ParseException("-topic must be provided");
        }
        return topic;
    }

    /**
     * Extracts bootstrap url.
     *
     * @param commandLine a CommandLine object.
     * @return bootstrap url.
     * @throws ParseException in case this arg is not present.
     */
    public static String getBootstrapUrl(CommandLine commandLine) throws ParseException {
        String bootstrapUrl = commandLine.getOptionValue(URL);
        if (bootstrapUrl == null) {
            throw new ParseException("-url must be provided");
        }
        return bootstrapUrl;
    }

    /**
     * Prints help.
     */
    public static void printHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Twitter kafka client.", OPTIONS);
    }
}
