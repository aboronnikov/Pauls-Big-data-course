package hdfs

import java.io.IOException

import scala.collection.{immutable, mutable}

/**
  * The entry point object of the program.
  */
object Main extends App {

  /**
    * This object provides constants for parsing input.
    */
  private object ArgConstants {
    /**
      * Schema path argument constant, used to specify the path of the schema file for conversion.
      */
    val SCHEMA_PATH_ARG = "-schemaPath"
    /**
      * Csv path argument constant, used to specify the path of the csv file to read from.
      */
    val CSV_PATH_ARG = "-csvPath"
    /**
      * New file name argument, used to specify the name of the new file, that will be created and written to.
      */
    val NEW_FILE_PATH_ARG = "-newFilePath"

    /**
      * Csv separator argument, used to specify what separator the csv file uses.
      */
    val CSV_SEPARATOR_ARG = "-csvSeparator"
    /**
      * Help argument, used to signal for help.
      */
    val HELP_ARG = "-help"
    /**
      * An example of an argument to this program is this: -schemaPath=/path/to/file, so "=" is used as a separator.
      */
    val KEY_VALUE_ARG_SEPARATOR = "="
    /**
      * This is a helper constant for validating input
      */
    val POSSIBLE_ARGS = immutable.HashSet(SCHEMA_PATH_ARG, CSV_PATH_ARG, NEW_FILE_PATH_ARG, CSV_SEPARATOR_ARG)
  }

  /**
    * This class provides constants for execution cases.
    */
  private object ExecutionCaseConstants {
    /**
      * User asked for help, in that case he must provide only 1 argument.
      */
    val HELP_CASE = 1
    /**
      * User knows how to use the program and provided all the necessary 4 arguments.
      */
    val NORMAL_CASE = 4
  }

  /**
    * This class provides constants for feedback messages.
    */
  private object FeedbackMessageConstants {
    /**
      * This is a help message to be displayed to the user.
      */
    val HELP_MESSAGE = "You have 2 options with this program:\n" +
      "1) Specify all 4 of 1) -schemaPath, 2) -csvPath, 3) -newFileName, 4) -csvSeparator\n" +
      "2) Ask for help with -help\n"
    /**
      * This is the bad arguments message to be displayed to the user.
      */
    val BAD_ARGS_MESSAGE = "You have provided bad arguments, you can specify \"-help\" to ask for help with this program"
    /**
      * This is the message to be displayed in case of an IO problem.
      */
    val IO_ERROR_MESSAGE = "There was a problem while reading from schema/csvFile, or writing to the parquet file"
    /**
      * The message is displayed when all operations have completed successfully.
      */
    val SUCCESS_MESSAGE = "Your csv has been successfully converted"
    /**
      * The message is displayed when an unaccounted exception is thrown.
      */
    val UNKNOWN_PROBLEM_MESSAGE = "An unknown problem occurred"
    /**
      * The message is displayed when the user provides a bad schema or a corrupted csv file.
      */
    val BAD_NUMBER_FORMAT_MESSAGE = "Either your file's schema is wrong or the contents of your csv file are corrupted"
  }

  /**
    * Processes arguments specified on command line.
    *
    * @param args the arguments specified.
    * @return the map with the processed arguments
    */
  private def processArgs(args: Array[String]): mutable.HashMap[String, String] = {
    val resultMap = new mutable.HashMap[String, String]

    /*
    If 4 arguments are passed, then they must be from the POSSIBLE_ARGS set.
     */
    if (args.length == ExecutionCaseConstants.NORMAL_CASE) {
      for (arg <- args) {
        val keyAndVal = arg.split(ArgConstants.KEY_VALUE_ARG_SEPARATOR)
        if (keyAndVal.size != 2 && !ArgConstants.POSSIBLE_ARGS.contains(keyAndVal(0))) {
          throw new IllegalArgumentException
        }
        resultMap(keyAndVal(0)) = keyAndVal(1)
      }
    }
    /*
    If only one argument is passed, then it can only be "-help"
     */
    else if (args.length == ExecutionCaseConstants.HELP_CASE && args(0).equals(ArgConstants.HELP_ARG)) {
      println(FeedbackMessageConstants.HELP_MESSAGE)
    }
    /*
    If the above 2 cases don't work out, then the arguments passed to this program are illegal.
     */
    else {
      throw new IllegalArgumentException
    }
    resultMap
  }

  /**
    * The entry point of the program.
    *
    * @param args command line args.
    */
  override
  def main(args: Array[String]): Unit = {
    try {
      val argMap = processArgs(args)

      if (argMap.isEmpty) {
        return
      }

      val schemaFilePath = argMap(ArgConstants.SCHEMA_PATH_ARG)
      val csvFilePath = argMap(ArgConstants.CSV_PATH_ARG)
      val newFileName = argMap(ArgConstants.NEW_FILE_PATH_ARG)
      val csvSeparator = argMap(ArgConstants.CSV_SEPARATOR_ARG)

      val csvToParquetConverter = new CsvToParquetConverter(schemaFilePath, csvFilePath, newFileName, csvSeparator)
      csvToParquetConverter.convertAndSaveAsANewFile()

      println(FeedbackMessageConstants.SUCCESS_MESSAGE)
    } catch {
      case e: NumberFormatException => println(FeedbackMessageConstants.BAD_NUMBER_FORMAT_MESSAGE)
      case e: IllegalArgumentException => println(FeedbackMessageConstants.BAD_ARGS_MESSAGE)
      case e: IOException => println(FeedbackMessageConstants.IO_ERROR_MESSAGE)
      case e: Exception => println(FeedbackMessageConstants.UNKNOWN_PROBLEM_MESSAGE)
    }
  }
}