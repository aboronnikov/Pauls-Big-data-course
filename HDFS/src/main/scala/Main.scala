/**
 * Main class that launches subprograms (hdfs, or what have you).
 */
object Main extends App {

  /**
   * Constants that I use for handling input.
   */
  object Constants {
    val DefaultMessage: String = "The first argument should be the name of the homework (hdfs)"
    val HdfsHomework: String = "hdfs"
  }

  override
  def main(args: Array[String]): Unit = {
    if (args.nonEmpty) {
      val firstArgument = args(0)
      firstArgument match {
        case Constants.HdfsHomework =>
          val numberOfArgs = args.length
          val actualArgs = args.slice(1, numberOfArgs)
          hdfs.Runner.run(actualArgs)
        case _                      => println(Constants.DefaultMessage)
      }
    } else {
      println(Constants.DefaultMessage)
    }
  }
}
