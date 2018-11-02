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
      .filter(keyValue => {val key = keyValue(0); ArgConstants.PossibleArgs.contains(key)})
      .map(keyValue => {val key = keyValue(0); val value = keyValue(1); key -> value})
      .toMap
  }

  /**
   * The entry point of the program.
   *
   * @param args command line args.
   */
  def main(args: Array[String]): Unit = {
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
      case e: IOException           => println(FeedbackMessageConstants.IOErrorMessage); println(e.getMessage)
      case e: Exception             => println(FeedbackMessageConstants.UnknownProblemMessage); println(e.getMessage)
    }
  }
}
