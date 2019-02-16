package com.epam.processingutils

/**
 * Command line utilities to parse command line args or print help.
 */
object CmdUtils {

  /**
   * String constants for cmd args.
   */
  val Format = "format"
  val Path = "path"
  val BootstrapServer = "bootstrapServer"
  val Topic = "topic"
  val StartingOffsets = "startingOffsets"
  val EndingOffsets = "endingOffsets"

  /**
   * These constants are only here for clarity.
   * HasArg means that an option has an argument, NoArg means the opposite.
   */
  private val HasArg = true
  private val NoArg = false

  /**
   * Options constant, comprises all the required options to be specified on the command line.
   */
  private val Options: Options = new Options()
    .addOption(Format, HasArg, "Format to write in.")
    .addOption(Path, HasArg, "Path to write into.")
    .addOption(BootstrapServer, HasArg, "Bootstrap server url.")
    .addOption(Topic, HasArg, "Topic name.")
    .addOption(StartingOffsets, HasArg, "Starting offsets.")
    .addOption(EndingOffsets, HasArg, "Ending offsets.")

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
    helpFormatter.printHelp("TopicDataIngester", Options)
  }

  /**
   * This method is a validity filter for cmd argument.
   *
   * @param cmdLine CommandLine object.
   * @return The same CommandLine object if it passes validity.
   */
  private def checkArgumentCorrectness(cmdLine: CommandLine): CommandLine = {
    val correctnessFlag =
      cmdLine.hasOption(Format) &&
        cmdLine.hasOption(Path) &&
        cmdLine.hasOption(BootstrapServer) &&
        cmdLine.hasOption(Topic) &&
        cmdLine.hasOption(StartingOffsets) &&
        cmdLine.hasOption(EndingOffsets)

    if (!correctnessFlag) {
      throw new ParseException("You didn't specify all arguments.")
    }
    cmdLine
  }
}
