package com.epam.processingutils

import org.apache.commons.cli.ParseException
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Tests the command line utility class CmdUtils.
 */
class CmdUtilsTest extends JUnitSuite {

  /**
   * Tests the parse method on bad arguments.
   * The method is supposed to throw an exception when something is wrong.
   */
  @Test(expected = classOf[ParseException])
  def parseTest(): Unit = {
    val cmdArgs = Array(
      "-format csv",
      "-path /path/to/file",
      "-bootstrapServer www.test.com",
      "-topic mytopic",
      // "-startingOffsets earliest", <- missing, so, throw exception
      "-endingOffsets"
    )
    CmdUtils.parse(cmdArgs)
  }

  /**
   * Tests that the parse method properly parses the arguments.
   */
  @Test
  def parseTest2(): Unit = {
    val cmdArgs = Array(
      "-format", "csv",
      "-path", "/path/to/file",
      "-bootstrapServer", "www.test.com",
      "-topic", "mytopic",
      "-startingOffsets", "earliest",
      "-endingOffsets", "latest"
    )
    val cmdLine = CmdUtils.parse(cmdArgs)

    val expectedFormat = "csv"
    val expectedPath = "/path/to/file"
    val expectedBootstrapServer = "www.test.com"
    val expectedTopic = "mytopic"
    val expectedStartingOffsets = "earliest"
    val expectedEndingOffsets = "latest"

    val actualFormat = cmdLine.getOptionValue(CmdUtils.Format)
    val actualPath = cmdLine.getOptionValue(CmdUtils.Path)
    val actualBootstrapServer = cmdLine.getOptionValue(CmdUtils.BootstrapServer)
    val actualTopic = cmdLine.getOptionValue(CmdUtils.Topic)
    val actualStartingOffsets = cmdLine.getOptionValue(CmdUtils.StartingOffsets)
    val actualEndingOffsets = cmdLine.getOptionValue(CmdUtils.EndingOffsets)

    Assert.assertEquals(expectedFormat, actualFormat)
    Assert.assertEquals(expectedPath, actualPath)
    Assert.assertEquals(expectedBootstrapServer, actualBootstrapServer)
    Assert.assertEquals(expectedTopic, actualTopic)
    Assert.assertEquals(expectedStartingOffsets, actualStartingOffsets)
    Assert.assertEquals(expectedEndingOffsets, actualEndingOffsets)
  }
}
