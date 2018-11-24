package com.epam.hdfs.inputprocessor

/**
 * This class provides constants for feedback messages.
 */
private object FeedbackMessageConstants {

  /**
   * This is the bad arguments message to be displayed to the user.
   */
  val BadArgsProblem: String = "You have provided bad arguments, you can specify \"-help\" " +
    "to ask for help with this program."

  /**
   * This is the message to be displayed in case of an IO problem.
   */
  val IOProblem: String = "There was a problem while reading from schema/csvFile, " +
    "or writing to the parquet file."

  /**
   * The message is displayed when all operations have completed successfully.
   */
  val Success: String = "Your csv has been successfully converted."

  /**
   * The message is displayed when an unaccounted exception is thrown.
   */
  val UnknownProblem: String = "An unknown problem occurred."

  /**
   * The message is displayed when the user provides a bad schema or a corrupted csv file.
   */
  val BadNumberFormat: String = "Either your file's schema is wrong " +
    "or the contents of your csv file are corrupted."
}
