package com.epam.processingutils

import org.apache.commons.cli.{CommandLine, HelpFormatter, Options, PosixParser}

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
    .addOption(Format, Format, HasArg, "Format to write in.")
    .addOption(Path, Path, HasArg, "Path to write into.")
    .addOption(BootstrapServer, BootstrapServer, HasArg, "Bootstrap server url.")
    .addOption(Topic, Topic, HasArg, "Topic name.")
    .addOption(StartingOffsets, StartingOffsets, HasArg, "Starting offsets.")
    .addOption(EndingOffsets, EndingOffsets, HasArg, "Ending offsets.")

  /**
   * Parses command line arguments into a command line object.
   *
   * @param args command line arguments.
   * @return returns the CommandLine object with parsed arguments.
   */
  def parse(args: Array[String]): CommandLine = {
    val parser = new PosixParser
    parser.parse(Options, args)
  }

  /**
   * Prints help for this program.
   */
  def printHelp(): Unit = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("TopicIngester", Options)
  }
}
