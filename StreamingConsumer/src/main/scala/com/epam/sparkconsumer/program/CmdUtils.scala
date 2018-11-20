package com.epam.sparkconsumer.program

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}

/**
 * Command line utilities.
 */
object CmdUtils {
  /**
   * Argument constants below.
   */
  val Topic = "topic"
  val Url = "url"
  val FilePath = "filePath"
  val FileFormat = "fileFormat"
  val Help = "help"

  private val HasArg = true
  private val NoArg = false

  /**
   * Possible command line options for this program.
   */
  private val Options = new Options()
    .addOption(Topic, HasArg, "Topic name. Required to run the program.")
    .addOption(Url, HasArg, "Topic url. Required to run the program.")
    .addOption(FilePath, HasArg, "File path. Required to run the program.")
    .addOption(FileFormat, HasArg, "File format. Required to run the program.")
    .addOption(Help, NoArg, "Help. Optional.")

  /**
   * Parses cmd arguments into a CommandLine object.
   *
   * @param args cmd args.
   * @return CommandLine object.
   */
  def parserArgs(args: Array[String]): CommandLine = {
    val cmdParser = new DefaultParser
    cmdParser.parse(Options, args)
  }

  /**
   * Checks if there are enough arguments to run this program.
   *
   * @param cmdLine command line object.
   * @return true or false, telling whether there are enough arguments.
   */
  def areArgumentsGood(cmdLine: CommandLine): Boolean = {
    cmdLine.hasOption(Topic) &&
      cmdLine.hasOption(Url) &&
      cmdLine.hasOption(FilePath) &&
      cmdLine.hasOption(FileFormat)
  }

  /**
   * Prints help.
   */
  def printHelp(): Unit = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("Consumer.", Options)
  }
}
