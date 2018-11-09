package com.epam.hdfs.inputprocessor

import java.io.IOException

import com.epam.hdfs.converter.CsvToParquetConverter
import org.apache.commons.cli._
import org.apache.log4j.Logger

/**
 * The entry point object of the program.
 */
object Runner {

  /**
   * The default logger for this class.
   */
  private val Log = Logger.getLogger(Runner.getClass)

  /**
   * Checks if there are enough arguments to run this program.
   *
   * @param cmdLine Cmd args.
   * @return Boolean telling whether or not there are enough arguments to run this program.
   */
  private def areThereEnoughArguments(cmdLine: CommandLine): Boolean = {
    cmdLine.hasOption(ArgConstants.SchemaPathArg) &&
      cmdLine.hasOption(ArgConstants.NewFilePathArg) &&
      cmdLine.hasOption(ArgConstants.CsvSeparatorArg) &&
      cmdLine.hasOption(ArgConstants.CsvPathArg)
  }

  /**
   * Takes action according to what user entered.
   * If they entered the 4 necessary arguments then runs the program.
   * If they asked for help, then prints the tool's manual.
   * If they didn't provide good arguments, then exits the program and tells about the bad arguments.
   *
   * @param cmdLine Cmd args.
   * @param options Possible cmd options.
   */
  private def takeActionAccordingToUserInput(cmdLine: CommandLine, options: Options): Unit = {
    if (cmdLine.hasOption(ArgConstants.HelpArg)) {
      val formatter = new HelpFormatter
      formatter.printHelp("CsvToParquetConverter", options)
    } else if (areThereEnoughArguments(cmdLine)) {
      val csvPath = cmdLine.getOptionValue(ArgConstants.CsvPathArg)
      val csvSeparator = cmdLine.getOptionValue(ArgConstants.CsvSeparatorArg)
      val newFilePath = cmdLine.getOptionValue(ArgConstants.NewFilePathArg)
      val schemaPath = cmdLine.getOptionValue(ArgConstants.SchemaPathArg)
      CsvToParquetConverter.convertAndSaveAsANewFile(csvPath, csvSeparator, newFilePath, schemaPath)
      Log.info(FeedbackMessageConstants.Success)
    } else {
      Log.info(FeedbackMessageConstants.BadArgsProblem)
    }
  }

  /**
   * The entry point of the program.
   *
   * @param args command line args.
   */
  def main(args: Array[String]): Unit = {
    try {
      val options = new Options
      options
        .addOption(ArgConstants.SchemaPathArg, true, "Specifies the path to the schema file. Required.")
        .addOption(ArgConstants.CsvPathArg, true, "Specified the path to the csv file. Required.")
        .addOption(ArgConstants.CsvSeparatorArg, true, "Specifies the separator used in the csv file. Required.")
        .addOption(ArgConstants.NewFilePathArg, true, "Specifies the path to the new parquet file. Required.")
        .addOption(ArgConstants.HelpArg, false, "Argument to call when you need help for this tool.")

      val cmdParser = new DefaultParser
      val cmdLine = cmdParser.parse(options, args)
      takeActionAccordingToUserInput(cmdLine, options)
    } catch {
      case e: NumberFormatException => Log.info(FeedbackMessageConstants.BadNumberFormat, e)
      case e: ParseException        => Log.info(FeedbackMessageConstants.BadArgsProblem, e)
      case e: IOException           => Log.info(FeedbackMessageConstants.IOProblem, e)
      case e: Exception             => Log.info(FeedbackMessageConstants.UnknownProblem, e)
    }
  }
}