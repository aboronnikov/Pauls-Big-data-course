package com.epam.hdfs.inputprocessor

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}

object CmdUtils {

  /**
   * Command line options available for this program.
   */
  val options: Options = new Options()
    .addOption(ArgConstants.SchemaPathArg, true, "Specifies the path to the schema file. Required.")
    .addOption(ArgConstants.CsvPathArg, true, "Specified the path to the csv file. Required.")
    .addOption(ArgConstants.CsvSeparatorArg, true, "Specifies the separator used in the csv file. Required.")
    .addOption(ArgConstants.NewFilePathArg, true, "Specifies the path to the new parquet file. Required.")
    .addOption(ArgConstants.HelpArg, false, "Argument to call when you need help for this tool.")

  /**
   * Parses user provided arguments.
   * @param args user input.
   * @return command line object.
   */
  def parseArgs(args: Array[String]): CommandLine = {
    val cmdParser = new DefaultParser
    cmdParser.parse(options, args)
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
