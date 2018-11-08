import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Class that solves task 1 of the spark module.
 */
object Task3 extends App {

  /**
   * Schema for the train.csv file.
   */
  val TrainSchema = new StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("date_time", StringType, nullable = true),
      StructField("site_name", IntegerType, nullable = true),
      StructField("posa_continent", IntegerType, nullable = true),
      StructField("user_location_country", IntegerType, nullable = true),
      StructField("user_location_region", IntegerType, nullable = true),
      StructField("user_location_city", IntegerType, nullable = true),
      StructField("orig_destination_distance", DoubleType, nullable = true),
      StructField("user_id", IntegerType, nullable = true),
      StructField("is_mobile", IntegerType, nullable = true),
      StructField("is_package", IntegerType, nullable = true),
      StructField("channel", IntegerType, nullable = true),
      StructField("srch_ci", StringType, nullable = true),
      StructField("srch_co", StringType, nullable = true),
      StructField("srch_adults_cnt", IntegerType, nullable = true),
      StructField("srch_children_cnt", IntegerType, nullable = true),
      StructField("srch_rm_cnt", IntegerType, nullable = true),
      StructField("srch_destination_id", IntegerType, nullable = true),
      StructField("srch_destination_type_id", IntegerType, nullable = true),
      StructField("hotel_continent", IntegerType, nullable = true),
      StructField("hotel_country", IntegerType, nullable = true),
      StructField("hotel_market", IntegerType, nullable = true),
      StructField("is_booking", IntegerType, nullable = true),
      StructField("cnt", LongType, nullable = true),
      StructField("hotel_cluster", IntegerType, nullable = true)
    ))

  /**
   * Reads datafrom specified csv.
   * @param pathToTrainCsv path to the csv that you want to read from.
   * @param spark current spark session.
   * @return DataFrame with data from csv.
   */
  def readDataFrameFromCsv(pathToTrainCsv: String, spark: SparkSession) : DataFrame = {
    spark.read
      .option("header", "true")
      .format("csv")
      .schema(TrainSchema)
      .load(pathToTrainCsv)
  }

  /**
   * Calculates the dataset as per task3 specification.
   * @param df dataframe with data from csv.
   * @return Dataset of results.
   */
  def calculateResults(df: DataFrame) : Dataset[Row] = {
    df.filter(df("srch_adults_cnt") > 0)
      .filter(df("srch_children_cnt") > 0)
      .filter(df("is_booking") === 0)
      .groupBy("hotel_country", "hotel_market", "hotel_continent")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
  }

  /**
   * Builds the spark session for processing the dataset.
   * @return new SparkSession.
   */
  def buildSession() : SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("SparkTask3")
      .getOrCreate()
  }

  /**
   * Program's entry point.
   * @param args cmd args.
   */
  override def main(args: Array[String]): Unit = {
    val spark = buildSession()
    val pathToTrainCsv = args(0)
    val df = readDataFrameFromCsv(pathToTrainCsv, spark)
    val dataset = calculateResults(df)
    dataset.show(3)
    spark.stop()
  }
}
