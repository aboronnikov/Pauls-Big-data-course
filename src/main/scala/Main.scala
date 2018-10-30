object Main extends App {

  object Constants {
    val DEFAULT_MESSAGE: String = "The first argument should be the name of the homework (hdfs)"
    val HDFS_HOMEWORK: String = "hdfs"
  }

  override
  def main(args: Array[String]): Unit = {
    if (args.length != 0) {
      args(0) match {
        case Constants.HDFS_HOMEWORK => hdfs.Runner.run(args.slice(1, args.length))
        case default => println(Constants.DEFAULT_MESSAGE)
      }
    } else {
      println(Constants.DEFAULT_MESSAGE)
    }
  }
}
