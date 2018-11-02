package hdfs.inputprocessor

/**
 * This class provides constants for execution cases.
 */
object ExecutionCaseConstants {
  /**
   * Case when the user knows how to use the program and provided all the necessary 4 arguments.
   */
  val NormalCase = 4
  /**
   * Case when the user either asked for help (-help flag), or provided bad arguments.
   */
  val HelpOrBadArgsCase = 1
}