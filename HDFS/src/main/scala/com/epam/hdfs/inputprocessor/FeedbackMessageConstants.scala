package com.epam.hdfs.inputprocessor

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