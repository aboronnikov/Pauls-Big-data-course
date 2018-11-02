package hdfs.inputprocessor
import java.io.IOException
import hdfs.converter.CsvToParquetConverter
import scala.collection.immutable

/**
 * The entry point object of the program.
 */
object Runner {

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
      .filter(keyValue => {
        val key = keyValue(0); ArgConstants.PossibleArgs.contains(key)
      })
      .map(keyValue => {
        val key = keyValue(0); val value = keyValue(1); key -> value
      })
      .toMap
  }

  /**
   * Checks if the user specified all the necessary arguments from ArgConstants.PossibleArgs.
   *
   * @param args cmd args
   * @return case id and argument map.
   */
  def checkForNormalCase(args: Array[String]): (Int, immutable.Map[String, String]) = {
    val argMap = processArgs(args)

    if (args.contains(ArgConstants.HelpArg)) {
      println(FeedbackMessageConstants.HelpMessage)
      (ExecutionCaseConstants.HelpOrBadArgsCase, immutable.Map.empty)
    } else if (argMap.size != ExecutionCaseConstants.NormalCase) {
      println(FeedbackMessageConstants.BadArgsMessage)
      (ExecutionCaseConstants.HelpOrBadArgsCase, immutable.Map.empty)
    } else {
      (ExecutionCaseConstants.NormalCase, argMap)
    }
  }

  /**
   * The entry point of the program.
   *
   * @param args command line args.
   */
  def main(args: Array[String]): Unit = {
    try {
      val processingResult = checkForNormalCase(args)
      val executionCase = processingResult._1
      val argumentMap = processingResult._2

      if (executionCase == ExecutionCaseConstants.NormalCase) {
        CsvToParquetConverter.convertAndSaveAsANewFile(argumentMap)
        println(FeedbackMessageConstants.SuccessMessage)
      }
    } catch {
      case e: NumberFormatException => println(FeedbackMessageConstants.BadNumberFormatMessage); println(e.getMessage)
      case e: IOException           => println(FeedbackMessageConstants.IOErrorMessage); println(e.getMessage)
      case e: Exception             => println(FeedbackMessageConstants.UnknownProblemMessage); println(e.getMessage)
    }
  }
}