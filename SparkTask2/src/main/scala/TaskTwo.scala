import org.apache.log4j.Logger

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Class that solves task 1 of the spark module.
 */
object TaskTwo {

  /**
   * Default logger.
   */
  val Log: Logger = Logger.getLogger(TaskTwo.getClass)

  /**
   * Below are all the header constants.
   */
  val Id: String = "id"
  val DateTime: String = "date_time"
  val SiteName: String = "site_name"
  val PosaContinent: String = "posa_continent"
  val UserLocationCountry: String = "user_location_country"
  val UserLocationRegion: String = "user_location_region"
  val UserLocationCity: String = "user_location_city"
  val OriginalDestinationDistance: String = "orig_destination_distance"
  val UserId: String = "user_id"
  val IsMobile: String = "is_mobile"
  val IsPackage: String = "is_package"
  val Channel: String = "channel"
  val SearchCheckIn: String = "srch_ci"
  val SearchCheckout: String = "srch_co"
  val SearchAdultsCount: String = "srch_adults_cnt"
  val SearchChildrenCount: String = "srch_children_cnt"
  val SearchRoomCount: String = "srch_rm_cnt"
  val SearchDestinationId: String = "srch_destination_id"
  val SearchDestinationTypeId: String = "srch_destination_type_id"
  val IsBooking: String = "is_booking"
  val HotelCluster: String = "hotel_cluster"
  val Cnt: String = "cnt"
  val HotelContinent: String = "hotel_continent"
  val HotelCountry: String = "hotel_country"
  val HotelMarket: String = "hotel_market"
  val Count: String = "count"

  /**
   * Schema for the train.csv file.
   */
  val TrainSchema = new StructType(Array(
    StructField(Id, IntegerType, nullable = true),
    StructField(DateTime, StringType, nullable = true),
    StructField(SiteName, IntegerType, nullable = true),
    StructField(PosaContinent, IntegerType, nullable = true),
    StructField(UserLocationCountry, IntegerType, nullable = true),
    StructField(UserLocationRegion, IntegerType, nullable = true),
    StructField(UserLocationCity, IntegerType, nullable = true),
    StructField(OriginalDestinationDistance, DoubleType, nullable = true),
    StructField(UserId, IntegerType, nullable = true),
    StructField(IsMobile, IntegerType, nullable = true),
    StructField(IsPackage, IntegerType, nullable = true),
    StructField(Channel, IntegerType, nullable = true),
    StructField(SearchCheckIn, StringType, nullable = true),
    StructField(SearchCheckout, StringType, nullable = true),
    StructField(SearchAdultsCount, IntegerType, nullable = true),
    StructField(SearchChildrenCount, IntegerType, nullable = true),
    StructField(SearchRoomCount, IntegerType, nullable = true),
    StructField(SearchDestinationId, IntegerType, nullable = true),
    StructField(SearchDestinationTypeId, IntegerType, nullable = true),
    StructField(HotelContinent, IntegerType, nullable = true),
    StructField(HotelCountry, IntegerType, nullable = true),
    StructField(HotelMarket, IntegerType, nullable = true),
    StructField(IsBooking, IntegerType, nullable = true),
    StructField(Cnt, LongType, nullable = true),
    StructField(HotelCluster, IntegerType, nullable = true)
  ))

  /**
   * Reads datafrom specified csv.
   *
   * @param pathToTrainCsv path to the csv that you want to read from.
   * @param spark          current spark session.
   * @return DataFrame with data from csv.
   */
  def readDataFrameFromCsv(pathToTrainCsv: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .format("csv")
      .schema(TrainSchema)
      .load(pathToTrainCsv)
  }

  /**
   * Builds the spark session for processing the DataFrame.
   *
   * @return new SparkSession.
   */
  def buildSession(): SparkSession = {
    SparkSession.builder()
      .appName("SparkTask2")
      .getOrCreate()
  }

  /**
   * Calculates the DataFrame as per task2 specification.
   *
   * @param df dataframe with data from csv.
   * @return DataFrame of results.
   */
  def calculateResults(df: DataFrame): DataFrame = {
    df.filter(df(HotelCountry) === df(SearchDestinationId))
      .filter(df(IsBooking) === 1)
      .groupBy(HotelCountry)
      .agg(count("*").alias(Count))
      .orderBy(desc(Count))
      .toDF()
  }

  /**
   * Program's entry point.
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val properLength = 1
    if (args.length == properLength) {
      val spark = buildSession()

      val firstArgument = 0
      val pathToTrainCsv = args(firstArgument)
      val df = readDataFrameFromCsv(pathToTrainCsv, spark)
      val result = calculateResults(df)
      val numberOfLinesToShow = 1
      result.show(numberOfLinesToShow)

      spark.stop()
    } else {
      Log.info("You must provide the path to your csv file.")
    }
  }
}
