import com.epam.utilities.DataFrameUtils
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
  * Tests for the DataFrameUtils class.
  */
class DataFrameUtilsTest extends JUnitSuite {

  /**
    * A test for the aggregateHourlyCountsWithWatermarking method.
    */
  @Test
  def aggregateHourlyCountsWithWatermarkingTest(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    import spark.implicits._

    val tweetDataTable = Seq(
      ("2019-01-16T16:08:59", "pavel", "12342142"),
      ("2019-01-16T16:08:59", "pavel", "12342142"),
      ("2019-01-16T16:08:59", "pavel", "12342142"),
      ("2019-01-16T16:08:59", "pavel1", "12342142")
    ).toDF("dateTime", "hashTag", "userId")

    val actual = DataFrameUtils.aggregateHourlyCountsWithWatermarking(spark, tweetDataTable)
    val expected = Seq(
      ("pavel", "12342142", "3"),
      ("pavel1", "12342142", "1")
    ).toDF("hashTag", "userId", "count")

    val difference1 = actual.select("hashTag", "userId", "count").except(expected)
    val difference2 = expected.except(actual.select("hashTag", "userId", "count"))

    Assert.assertTrue(difference1.count() == difference2.count())
  }
}
