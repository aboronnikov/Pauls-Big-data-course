import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test suite for task3.
 */
class Task3Test extends JUnitSuite {

  /**
   * Spark session necessary for this task.
   */
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkTask3")
    .getOrCreate()

  import spark.implicits._

  /**
   * A test dataframe.
   * In this dataframe the result for task3 will be: 3,1,1
   * because the first group of hotels has 3 groups of people with children,
   * the second group of hotels has 1 group of people with children,
   * and the third group of hotels has 1 group of people with children also.
   */
  private val dataFrame = Seq(
    (2, 3, 0, 1, 1, 1),
    (2, 3, 0, 1, 1, 1),
    (2, 3, 0, 1, 1, 1),
    (2, 3, 0, 2, 2, 2),
    (2, 3, 0, 3, 3, 3)
  ).toDF(Task3.SrchAdultsCnt, Task3.SrchChildrenCnt, Task3.IsBooking, Task3.HotelCountry, Task3.HotelMarket, Task3.HotelContinent)

  /**
   * A test, comparing the answer of 3,1,1 with whatever calculateResults produces.
   */
  @Test
  def calculateResultsTest(): Unit = {
    val dataset = Task3.calculateResults(dataFrame)
    val expected = Array[Long](3, 1, 1)
    val actual = dataset.select(Task3.Count).as[Long].collect()
    Assert.assertArrayEquals(expected, actual)
  }

  /**
   * This is a necessary procedure of stopping spark session to clean up resources.
   */
  @After
  def tearDown(): Unit = {
    spark.stop()
  }
}
