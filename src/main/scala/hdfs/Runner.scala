package hdfs

import java.io.IOException

import scala.collection.immutable

/**
  * The entry point object of the program.
  */
object Runner {

  /**
    * This object provides constants for parsing input.
    */
  private object ArgConstants {
    /**
      * Schema path argument constant, used to specify the path of the schema file for conversion.
      */
    val SchemaPathArg = "-schemaPath"
    /**
      * Csv path argument constant, used to specify the path of the csv file to read from.
      */
    val CsvPathArg = "-csvPath"
    /**
      * New file name argument, used to specify the name of the new file, that will be created and written to.
      */
    val NewFilePathArg = "-newFilePath"

    /**
      * Csv separator argument, used to specify what separator the csv file uses.
      */
    val CsvSeparatorArg = "-csvSeparator"
    /**
      * Help argument, used to signal for help.
      */
    val HelpArg = "-help"
    /**
      * An example of an argument to this program is this: -schemaPath=/path/to/file, so "=" is used as a separator.
      */
    val KeyValueArgSeparator = "="
    /**
      * This is a helper constant for validating input
      */
    val PossibleArgs = immutable.HashSet(SchemaPathArg, CsvPathArg, NewFilePathArg, CsvSeparatorArg)
  }

  /**
    * This class provides constants for execution cases.
    */
  private object ExecutionCaseConstants {
    /**
      * User knows how to use the program and provided all the necessary 4 arguments.
      */
    val NormalCase = 4
  }

  /**
    * This class provides constants for feedback messages.
    */
  private object FeedbackMessageConstants {
    /**
      * This is a help message to be displayed to the user.
      */
    val HelpMessage = "You have 2 options with this program:\n" +
      "1) Specify all 4 of 1) -schemaPath, 2) -csvPath, 3) -newFilePath, 4) -csvSeparator\n" +
      "2) Ask for help with -help\n"
    /**
      * This is the bad arguments message to be displayed to the user.
      */
    val BadArgsMessage = "You have provided bad arguments, you can specify \"-help\" to ask for help with this program"
    /**
      * This is the message to be displayed in case of an IO problem.
      */
    val IOErrorMessage = "There was a problem while reading from schema/csvFile, or writing to the parquet file"
    /**
      * The message is displayed when all operations have completed successfully.
      */
    val SuccessMessage = "Your csv has been successfully converted"
    /**
      * The message is displayed when an unaccounted exception is thrown.
      */
    val UnknownProblemMessage = "An unknown problem occurred"
    /**
      * The message is displayed when the user provides a bad schema or a corrupted csv file.
      */
    val BadNumberFormatMessage = "Either your file's schema is wrong or the contents of your csv file are corrupted"
  }

  /**
    * Processes arguments specified on command line.
    * If this function returns a map with ExecutionCaseConstants.NormalCase number of elements
    * then schemaFilePath, csvFilePath, newFileName, csvSeparator are in there.
    *
    * @param args the arguments specified.
    * @return the map with the processed arguments
    */
  private def processArgs(args: Array[String]): immutable.Map[String, String] = {
    args
      .filter(arg => arg.contains(ArgConstants.KeyValueArgSeparator))
      .map(arg => arg.split(ArgConstants.KeyValueArgSeparator))
      .filter(keyValue => ArgConstants.PossibleArgs.contains(keyValue(0)))
      .map(keyValue => keyValue(0) -> keyValue(1))
      .toMap
  }

  /**
    * The entry point of the program.
    *
    * @param args command line args.
    */
  def run(args: Array[String]): Unit = {
    try {
      val argMap = processArgs(args)

      if (argMap.size == ExecutionCaseConstants.NormalCase) {
        val schemaFilePath = argMap(ArgConstants.SchemaPathArg)
        val csvFilePath = argMap(ArgConstants.CsvPathArg)
        val newFileName = argMap(ArgConstants.NewFilePathArg)
        val csvSeparator = argMap(ArgConstants.CsvSeparatorArg)

        CsvToParquetConverter.convertAndSaveAsANewFile(schemaFilePath, csvFilePath, newFileName, csvSeparator)

        println(FeedbackMessageConstants.SuccessMessage)
      } else if (argMap.contains(ArgConstants.HelpArg)) {
        println(FeedbackMessageConstants.HelpMessage)
      } else {
        println(FeedbackMessageConstants.BadArgsMessage)
      }
    } catch {
      case e: NumberFormatException => println(FeedbackMessageConstants.BadNumberFormatMessage); println(e.getMessage)
      case e: IOException => println(FeedbackMessageConstants.IOErrorMessage); println(e.getMessage)
      case e: Exception => println(FeedbackMessageConstants.UnknownProblemMessage); println(e.getMessage)
    }
  }
}