package com.epam.hdfs.inputprocessor

/**
 * This object provides constants for parsing input.
 */
private object ArgConstants {
  /**
   * Schema path argument constant, used to specify the path of the schema file for conversion.
   */
  val SchemaPathArg = "schemaPath"
  /**
   * Csv path argument constant, used to specify the path of the csv file to read from.
   */
  val CsvPathArg = "csvPath"
  /**
   * New file name argument, used to specify the name of the new file,
   * that will be created and written to.
   */
  val NewFilePathArg = "newFilePath"
  /**
   * Csv separator argument, used to specify what separator the csv file uses.
   */
  val CsvSeparatorArg = "csvSeparator"
  /**
   * Help argument, used to signal for help.
   */
  val HelpArg = "help"
}
