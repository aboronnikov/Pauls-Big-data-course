package com.epam.hdfs.inputprocessor

import java.io.IOException

import com.epam.hdfs.converter.CsvToParquetConverter
import org.apache.commons.cli._
import org.apache.log4j.Logger
import resource.managed
import scala.io.Source

/**
 * The entry point object of the program.
 */
object Runner {

  /**
   * The default logger for this class.
   */
  private val log = Logger.getLogger(Runner.getClass)

  /**
   * The main function of this utility that converts csv to parquet format.
   */
  private def convertCsvToParquet(csvFilePath: String, csvSeparator: String, newFilePath: String, schemaFilePath: String): Unit = {
    val schema = IOUtils.readSchemaFromFile(schemaFilePath)
    for {
      source <- managed(Source.fromFile(csvFilePath))
    } {
      val fileStream = source.getLines
      val groupStream = CsvToParquetConverter.transformIntoGroupStream(fileStream, csvSeparator, schema)
      IOUtils.writeGroupsToFile(groupStream, newFilePath, schema)
    }
  }

  /**
   * Takes action according to what user entered.
   * If they entered the 4 necessary arguments then runs the program.
   * If they asked for help, then prints the tool's manual.
   * If they didn't provide good arguments, then exits the program and tells about the bad arguments.
   *
   * @param args user input
   */
  def main(args: Array[String]): Unit = {
    try {
      val cmdLine = CmdUtils.parseArgs(args)
      if (cmdLine.hasOption(ArgConstants.HelpArg)) {
        val formatter = new HelpFormatter
        formatter.printHelp("CsvToParquetConverter", CmdUtils.Options)
      } else if (CmdUtils.areThereEnoughArguments(cmdLine)) {
        val csvPath = cmdLine.getOptionValue(ArgConstants.CsvPathArg)
        val csvSeparator = cmdLine.getOptionValue(ArgConstants.CsvSeparatorArg)
        val newFilePath = cmdLine.getOptionValue(ArgConstants.NewFilePathArg)
        val schemaPath = cmdLine.getOptionValue(ArgConstants.SchemaPathArg)
        convertCsvToParquet(csvPath, csvSeparator, newFilePath, schemaPath)
        log.info(FeedbackMessageConstants.Success)
      } else {
        log.info(FeedbackMessageConstants.BadArgsProblem)
      }
    } catch {
      case e: NumberFormatException => log.info(FeedbackMessageConstants.BadNumberFormat, e)
      case e: ParseException        => log.info(FeedbackMessageConstants.BadArgsProblem, e)
      case e: IOException           => log.info(FeedbackMessageConstants.IOProblem, e)
    }
  }
}
