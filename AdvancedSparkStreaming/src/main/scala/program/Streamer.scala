package program

import com.epam.utilities.{CmdUtils, DataFrameUtils, KafkaIOUtils}
import org.apache.commons.cli.ParseException
import org.apache.spark.sql.SparkSession

/**
  * Entry point class.
  */
object Streamer {
  /**
    * Entry point method of our program.
    *
    * @param args cmd args.
    */
  def main(args: Array[String]): Unit = {

    try {
      val cmdLine = CmdUtils.parse(args)

      val bootstrapServer = cmdLine.getOptionValue(CmdUtils.BootstrapServer)
      val inputTopic = cmdLine.getOptionValue(CmdUtils.InputTopic)
      val outputTopic = cmdLine.getOptionValue(CmdUtils.OutputTopic)
      val startingOffsets = cmdLine.getOptionValue(CmdUtils.StartingOffsets)
      val checkPointLocation = cmdLine.getOptionValue(CmdUtils.CheckPointLocation)

      ingestData(bootstrapServer, inputTopic, outputTopic, startingOffsets, checkPointLocation)
    } catch {
      case _: ParseException => CmdUtils.printHelp()
    }
  }

  /**
    * Ingests data from one kafka topic into the other.
    *
    * @param bootstrapServer    bootstrap server associated with the topics.
    * @param inputTopic         the topic to read from.
    * @param outputTopic        the topic to write into.
    * @param startingOffsets    starting offsets in the input topic.
    * @param checkPointLocation check pointing folder path.
    */
  private def ingestData(bootstrapServer: String,
                         inputTopic: String,
                         outputTopic: String,
                         startingOffsets: String,
                         checkPointLocation: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Streamer")
      .getOrCreate()

    val tweetDataTable = KafkaIOUtils.readStreamIntoDataFrame(spark, bootstrapServer, inputTopic, startingOffsets)
    val aggregatedByHourDF = DataFrameUtils.aggregateHourlyCountsWithWatermarking(spark, tweetDataTable)
    val kafkaOutputStream = KafkaIOUtils.streamIntoKafka(spark, aggregatedByHourDF, checkPointLocation, bootstrapServer, outputTopic)

    kafkaOutputStream
      .awaitTermination()

    spark.stop()
  }

}
