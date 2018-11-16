import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test suite for task1.
 */
class TaskOneTest extends JUnitSuite {

  /**
   * Spark session necessary for this task.
   */
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkTask1")
    .getOrCreate()

  import spark.implicits._

  /**
   * A test dataframe.
   * In this dataframe the result for task1 will be: 2,1,1,1.
   * because the first group of hotels has 1 couple,
   * the second group of hotels has 1 couple,
   * ...
   * the last one has 2 couples.
   * Put the above stuff in descending order and you get 2,1,1,1.
   */
  private val dataFrame = Seq(
    (2, 1, 1, 1),
    (2, 2, 2, 2),
    (2, 3, 3, 3),
    (2, 4, 4, 4),
    (2, 4, 4, 4)
  ).toDF(TaskOne.SrchAdultsCnt, TaskOne.HotelCountry, TaskOne.HotelMarket, TaskOne.HotelContinent)

  /**
   * A test, comparing the answer of 2,1,1,1 with whatever calculateResults produces.
   */
  @Test
  def calculateResultsTest(): Unit = {
    val dataset = TaskOne.calculateResults(dataFrame)
    val expected = Array[Long](2, 1, 1, 1)
    val actual = dataset.select(TaskOne.Count).as[Long].collect()
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
