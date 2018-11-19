import org.apache.spark.sql.SparkSession

/**
  * A simple consumer that reads from our kafka topic, using spark streaming.
  */
object Consumer {
  /**
    * The entry point.
    * @param args cmd args.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .getOrCreate()

    val topic = args(0)
    val url = args(1)
    val fileName = args(2)
    val fileFormat = args(4)

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", url)
      .option("subscribe", topic)
      .load()
      .write
      .format(fileFormat)
      .save(fileName)

    spark.stop()
  }
}
