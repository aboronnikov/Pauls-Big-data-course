import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkTask2")
      .getOrCreate()

    val trainSchema = new StructType(Array(
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

    val pathToTrainCsv = args(0)

    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(trainSchema)
      .load(pathToTrainCsv)

    df.filter(df("hotel_country") === df("srch_destination_id"))
      .filter(df("is_booking") === 1)
      .groupBy("hotel_country")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
      .show(1)

    spark.stop()
  }
}
