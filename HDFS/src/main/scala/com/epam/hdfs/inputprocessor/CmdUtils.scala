package com.epam.hdfs.inputprocessor

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}

object CmdUtils {

  private val HasArg = true
  private val NoArg = true
  /**
   * Command line options available for this program.
   */
  val Options: Options = new Options()
    .addOption(ArgConstants.SchemaPathArg, HasArg, "Specifies the path to the schema file. Required.")
    .addOption(ArgConstants.CsvPathArg, HasArg, "Specified the path to the csv file. Required.")
    .addOption(ArgConstants.CsvSeparatorArg, HasArg, "Specifies the separator used in the csv file. Required.")
    .addOption(ArgConstants.NewFilePathArg, HasArg, "Specifies the path to the new parquet file. Required.")
    .addOption(ArgConstants.HelpArg, NoArg, "Argument to call when you need help for this tool.")

  /**
   * Parses user provided arguments.
   * @param args user input.
   * @return command line object.
   */
  def parseArgs(args: Array[String]): CommandLine = {
    val cmdParser = new DefaultParser
    cmdParser.parse(Options, args)
  }

  /**
   * Checks if there are enough arguments to run this program.
   *
   * @param cmdLine Cmd args.
   * @return Boolean telling whether or not there are enough arguments to run this program.
   */
  def areThereEnoughArguments(cmdLine: CommandLine): Boolean = {
    cmdLine.hasOption(ArgConstants.SchemaPathArg) &&
      cmdLine.hasOption(ArgConstants.NewFilePathArg) &&
      cmdLine.hasOption(ArgConstants.CsvSeparatorArg) &&
      cmdLine.hasOption(ArgConstants.CsvPathArg)
  }
}
