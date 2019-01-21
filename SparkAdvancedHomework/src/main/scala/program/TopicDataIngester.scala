package program

import com.epam.processingutils.{CmdUtils, DataFrameUtils}
import com.epam.spark.extensions.FileSystemExtensions._
import com.epam.spark.extensions.SparkExtensions._
import org.apache.commons.cli.ParseException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Entry point class.
 * Reads the necessary command line arguments and utilizes the DataFrameProcessor class
 * to read the new data from kafka and merge it with the already existing on the HDFS data.
 */
object TopicDataIngester {

  /**
   * String constants used in this class.
   */
  val Date = "date"
  val Hour = "hour"
  val HashTag = "hashTag"
  val UserId = "userId"
  val Count = "cnt"

  /**
   * This is the schema specifying all the fields we need to store into the hdfs.
   */
  val TweetSchema: StructType =
    new StructType(Array(
      StructField(Date, StringType, nullable = true),
      StructField(Hour, StringType, nullable = true),
      StructField(HashTag, StringType, nullable = true),
      StructField(UserId, StringType, nullable = true),
      StructField(Count, IntegerType, nullable = true)
    ))

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

  /**
   * Merges the new data with the already existing ones.
   *
   * 1) This method first reads the new data from kafka.
   * 2) Then it figures out which of already existing partitions it will have to update.
   * 3) Then it loads up those partitions from disk.
   * 4) In the very end it merges the old data with the new data and writes the results to the HDFS.
   *
   * @param format          format of data we're working with
   * @param basePath        path to the data directory
   * @param bootstrapServer bootstrap server url
   * @param topic           kafka topic to read from
   * @param startingOffsets starting offsets in the kafka topic
   * @param endingOffsets   ending offsets in the kafka topic
   */
  private def ingestDataFromKafka(format: String,
                                  basePath: String,
                                  bootstrapServer: String,
                                  topic: String,
                                  startingOffsets: String,
                                  endingOffsets: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Ingester")
      .getOrCreate()
    val fs = FileSystem.get(new Configuration)

    mergeKafkaIntoHDFS(spark, bootstrapServer, topic, startingOffsets, endingOffsets, format, basePath, fs)

    spark.close()
    fs.close()
  }

  /**
   * Merges the new data with the already existing ones.
   *
   * 1) This method first reads the new data from kafka.
   * 2) Then it figures out which of already existing partitions it will have to update.
   * 3) Then it loads up those partitions from disk.
   * 4) In the very end it merges the old data with the new data and writes the results to the HDFS.
   *
   * @param spark           spark
   * @param bootstrapServer bootstrapServer
   * @param topic           topic
   * @param startingOffsets startingOffsets
   * @param endingOffsets   endingOffsets
   * @param format          format
   * @param basePath        basePath
   * @param fs              fs
   */
  private def mergeKafkaIntoHDFS(spark: SparkSession,
                                 bootstrapServer: String,
                                 topic: String,
                                 startingOffsets: String,
                                 endingOffsets: String,
                                 format: String,
                                 basePath: String,
                                 fs: FileSystem): Unit = {
    // cached, because will need to be reused.
    val newDF = readKafkaData(spark, bootstrapServer, topic, startingOffsets, endingOffsets).cache()
    val paths = DataFrameUtils.extractIntersectionPaths(newDF)
    val oldDF = spark.readDataFrameFromHDFS(format, basePath, paths, fs, TweetSchema)
    fs.removeCorrespondingPaths(basePath, paths)
    val unifiedDF = makeUnifiedData(spark, oldDF, newDF)
    saveToHDFSByDateAndHour(spark, unifiedDF, basePath, format)
  }

  /**
   * Reads the necessary data from kafka
   *
   * @param spark           sparkSession
   * @param bootstrapServer bootstrapServer url
   * @param topic           topic
   * @param startingOffsets starting offsets for kafka
   * @param endingOffsets   ending offsets for kafka
   * @return new dataframe
   */
  private def readKafkaData(spark: SparkSession,
                            bootstrapServer: String,
                            topic: String,
                            startingOffsets: String,
                            endingOffsets: String): DataFrame = {
    val columnNames = Array(Date, Hour, HashTag, UserId)
    spark.readTweetDataFrameFromKafka(
      bootstrapServer,
      topic,
      startingOffsets,
      endingOffsets,
      columnNames,
      Count
    ).cache()
  }

  /**
   * Unifies the new kafka data and the old hdfs data.
   *
   * @param spark sparkSession
   * @param oldDF oldDF with the HDFS data.
   * @param newDF newDF with the new data that we received from kafka.
   * @return dataframe with the new data.
   */
  private def makeUnifiedData(spark: SparkSession, oldDF: DataFrame, newDF: DataFrame): DataFrame = {
    val groupByArray = Array(Date, Hour, HashTag, UserId)
    spark.mergeRunningTotals(oldDF, newDF, groupByArray, Count)
  }

  /**
   * Saves the data to the HDFS, partitioned by data and hour.
   *
   * @param spark     sparkSession.
   * @param unifiedDF the unification of old and new data.
   * @param basePath  the path to the data dir.
   * @param format    format of the data.
   */
  private def saveToHDFSByDateAndHour(spark: SparkSession, unifiedDF: DataFrame, basePath: String, format: String): Unit = {
    val partitions = Array(Date, Hour)
    spark.writeDataFrameToHDFS(unifiedDF, basePath, format, partitions, SaveMode.Append)
  }
}
