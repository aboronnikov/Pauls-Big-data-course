package com.epam.hdfs.inputprocessor
import scala.collection.immutable

/**
 * This object provides constants for parsing input.
 */
object ArgConstants {
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