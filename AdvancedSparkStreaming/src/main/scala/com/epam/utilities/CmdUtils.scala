package com.epam.utilities

import org.apache.commons.cli._

/**
  * Command line utilities to parse command line args or print help.
  */
object CmdUtils {
  /**
    * String constants for cmd args.
    */
  val BootstrapServer = "bootstrapServer"
  val InputTopic = "inputTopic"
  val OutputTopic = "outputTopic"
  val StartingOffsets = "startingOffsets"
  val CheckPointLocation = "checkPointLocation"

  /**
    * These constants are only here for clarity.
    * HasArg means that an option has an argument, NoArg means the opposite.
    */
  private val HasArg = true
  private val NoArg = false

  /**
    * Options constant, comprises all the required options to be specified on the command line.
    */
  private val Options = new Options()
    .addOption(BootstrapServer, HasArg, "Bootstrap server for topics.")
    .addOption(InputTopic, HasArg, "Input topic to read from.")
    .addOption(OutputTopic, HasArg, "Output topic to write to.")
    .addOption(StartingOffsets, HasArg, "Starting offsets for our input topic.")
    .addOption(CheckPointLocation, HasArg, "Check point location.")

  /**
    * Parses command line arguments into a command line object.
    *
    * @param args command line arguments.
    * @return returns the CommandLine object with parsed arguments.
    */
  def parse(args: Array[String]): CommandLine = {
    val parser = new PosixParser
    val cmdLine = parser.parse(Options, args)
    checkArgumentCorrectness(cmdLine)
  }

  /**
    * Prints help for this program.
    */
  def printHelp(): Unit = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("Streamer", Options)
  }

  /**
    * This method is a validity filter for cmd argument.
    *
    * @param cmdLine CommandLine object.
    * @return The same CommandLine object if it passes validity.
    */
  private def checkArgumentCorrectness(cmdLine: CommandLine): CommandLine = {
    val correctnessFlag =
      cmdLine.hasOption(BootstrapServer) &&
        cmdLine.hasOption(InputTopic) &&
        cmdLine.hasOption(OutputTopic) &&
        cmdLine.hasOption(StartingOffsets) &&
        cmdLine.hasOption(CheckPointLocation)

    if (!correctnessFlag) {
      throw new ParseException("You didn't specify all arguments.")
    }
    cmdLine
  }

}
