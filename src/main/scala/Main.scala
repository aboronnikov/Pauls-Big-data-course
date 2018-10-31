object Main extends App {

  object Constants {
    val DefaultMessage: String = "The first argument should be the name of the homework (hdfs)"
    val HdfsHomework: String = "hdfs"
  }

  override
  def main(args: Array[String]): Unit = {
    if (args.length != 0) {
      args(0) match {
        case Constants.HdfsHomework =>
          val actualArgs = args.slice(1, args.length)
          hdfs.Runner.run(actualArgs)
        case _                      => println(Constants.DefaultMessage)
      }
    } else {
      println(Constants.DefaultMessage)
    }
  }
}
