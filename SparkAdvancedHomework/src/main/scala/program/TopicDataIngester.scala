package program

import com.epam.processingutils.CmdUtils
import org.apache.commons.cli.ParseException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
  * Entry point class.
  * Reads the necessary command line arguments and utilizes the DataFrameProcessor class
  * to read the new data from kafka and merge it with the already existing on the HDFS data.
  */
object TopicDataIngester {

  /**
    * Merges the new data with the already existing ones.
    *
    * 1) This method first reads the new data from kafka.
    * 2) Then it figures out which of already existing partitions it will have to update.
    * 3) Then it loads up those partitions from disk.
    * 4) In the very end it merges the old data with the new data and writes the results to the HDFS.
    *
    * @param format          format of data we're working with
    * @param path            path to write to/read from
    * @param bootstrapServer bootstrap server url
    * @param topic           kafka topic to read from
    * @param startingOffsets starting offsets in the kafka topic
    * @param endingOffsets   ending offsets in the kafka topic
    */
  def ingestDataFromKafka(format: String,
                          path: String,
                          bootstrapServer: String,
                          topic: String,
                          startingOffsets: String,
                          endingOffsets: String): Unit = {
    import com.epam.spark.extensions.SparkExtensions._

    val spark = SparkSession.builder()
      .appName("Ingester")
      .getOrCreate()

    val fs = FileSystem.get(new Configuration)

    // cached, because will need to be reused.
    val newDF = spark.readDataFrameFromKafka(bootstrapServer, topic, startingOffsets, endingOffsets).cache()
    val paths = spark.extractIntersectionPaths(newDF)

    val oldDF = spark.loadDataFrameFromHDFS(format, path, paths, fs)
    val unifiedDF = spark.mergeRunningTotals(oldDF, newDF)
    spark.writeDataFrameToHDFS(unifiedDF, path, format)

    spark.close()
    fs.close()
  }

  /**
    * Program's entry point.
    *
    * @param args cmd args.
    */
  def main(args: Array[String]): Unit = {
    try {
      val cmdLine = CmdUtils.parse(args)

      val format = cmdLine.getOptionValue(CmdUtils.Format)
      val path = cmdLine.getOptionValue(CmdUtils.Path)
      val bootstrapServer = cmdLine.getOptionValue(CmdUtils.BootstrapServer)
      val topic = cmdLine.getOptionValue(CmdUtils.Topic)
      val startingOffsets = cmdLine.getOptionValue(CmdUtils.StartingOffsets)
      val endingOffsets = cmdLine.getOptionValue(CmdUtils.EndingOffsets)

      ingestDataFromKafka(format, path, bootstrapServer, topic, startingOffsets, endingOffsets)
    } catch {
      case _: ParseException => CmdUtils.printHelp()
    }
  }
}
