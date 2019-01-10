package com.epam.program;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for CmdUtils.
 * Methods with suffix "2" test failures.
 */
public class CmdUtilsTest {

    /**
     * Necessary for tests.
     */
    private CommandLine commandLine;
    private CommandLine badCommandLine;

    /**
     * Set up the command line object.
     *
     * @throws ParseException which is ignored.
     */
    @Before
    public void setUp() throws ParseException {
        String[] args = {"-topic", "hadoop", "-url", "sandbox-hdp.hortonworks.com:6667", "-passStore", "C:/One,Two", "-keywords", "pavel1,pavel2,pavel3"};
        commandLine = CmdUtils.parseArgs(args);
        String[] args2 = {};
        badCommandLine = CmdUtils.parseArgs(args2);
    }

    /**
     * Tests the method with the same name.
     */
    @Test
    public void parseArgs() {
        //I use the commandLine object create in the setUp method.

        Assert.assertEquals("hadoop", commandLine.getOptionValue(CmdUtils.TOPIC));
        Assert.assertEquals("sandbox-hdp.hortonworks.com:6667", commandLine.getOptionValue(CmdUtils.URL));
        Assert.assertEquals("C:/One,Two", commandLine.getOptionValue(CmdUtils.PASS_STORE));
        Assert.assertEquals("pavel1,pavel2,pavel3", commandLine.getOptionValue(CmdUtils.KEYWORDS));
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test
    public void getKeywords() throws ParseException {
        List<String> expected = Arrays.asList("pavel1", "pavel2", "pavel3");
        List<String> actual = CmdUtils.getKeywords(commandLine);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test(expected = ParseException.class)
    public void getKeywords2() throws ParseException {
        CmdUtils.getKeywords(badCommandLine);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test
    public void getPassStore() throws ParseException {
        String expected = "C:/One,Two";
        String actual = CmdUtils.getPassStore(commandLine);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test(expected = ParseException.class)
    public void getPassStore2() throws ParseException {
        CmdUtils.getPassStore(badCommandLine);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test
    public void getTopic() throws ParseException {
        String expected = "hadoop";
        String actual = CmdUtils.getTopic(commandLine);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test(expected = ParseException.class)
    public void getTopic2() throws ParseException {
        CmdUtils.getTopic(badCommandLine);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test
    public void getBootstrapUrl() throws ParseException {
        String expected = "sandbox-hdp.hortonworks.com:6667";
        String actual = CmdUtils.getBootstrapUrl(commandLine);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     *
     * @throws ParseException parsing exception that is ignored.
     */
    @Test(expected = ParseException.class)
    public void getBootstrapUrl2() throws ParseException {
        CmdUtils.getBootstrapUrl(badCommandLine);
    }
}