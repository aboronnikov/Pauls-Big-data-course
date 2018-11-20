package com.epam.sparkproducer.program

import org.apache.commons.cli._
import org.apache.commons.lang3.math.NumberUtils

/**
 * Command line utilities.
 */
object CmdUtils {
  /**
   * Argument constants below.
   */
  val Topic = "topic"
  val Url = "url"
  val NThreads = "nThreads"
  val FilePath = "filePath"
  val Help = "help"

  private val HasArg = true
  private val NoArg = false

  /**
   * Possible command line options for this program.
   */
  private val Options = new Options()
    .addOption(Topic, HasArg, "Topic name. Required to run the program.")
    .addOption(Url, HasArg, "Topic url. Required to run the program.")
    .addOption(NThreads, HasArg, "Number of threads. Required to run the program.")
    .addOption(FilePath, HasArg, "File path. Required to run the program.")
    .addOption(Help, NoArg, "Help. Optional.")

  /**
   * Parses cmd arguments into a CommandLine object.
   *
   * @param args cmd args.
   * @return CommandLine object.
   */
  def parserArgs(args: Array[String]): CommandLine = {
    val cmdParser = new PosixParser
    cmdParser.parse(Options, args)
  }

  /**
   * Checks if there are enough arguments to run this program.
   * Also checks that nTheads is a number.
   *
   * @param cmdLine command line object.
   * @return true of false, telling whether there are enough arguments.
   */
  def areArgumentsGood(cmdLine: CommandLine): Boolean = {
    val allArgumentsArePresent =
      cmdLine.hasOption(Topic) &&
        cmdLine.hasOption(Url) &&
        cmdLine.hasOption(NThreads) &&
        cmdLine.hasOption(FilePath)

    val nThreadsStr = cmdLine.getOptionValue(NThreads)
    val nThreadsIsAllDigits = NumberUtils.isDigits(nThreadsStr)

    allArgumentsArePresent && nThreadsIsAllDigits
  }

  /**
   * Prints help.
   */
  def printHelp(): Unit = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("Producer.", Options)
  }
}
