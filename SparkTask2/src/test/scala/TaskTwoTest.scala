import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test suite for task2.
 */
class TaskTwoTest extends JUnitSuite {

  /**
   * Spark session necessary for this task.
   */
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkTask2")
    .getOrCreate()

  import spark.implicits._

  /**
   * A test dataframe.
   * In the first group of searches, we have 2 searches for hotel country 1.
   * In the second group of searches, we have 1 search for hotel country 2.
   * The expected answer is: 2,1.
   */
  private val dataFrame = Seq(
    (1, 1, 1),
    (1, 1, 1),
    (2, 2, 1),
  ).toDF(TaskTwo.HotelCountry, TaskTwo.SrchDestinationId, TaskTwo.IsBooking)

  /**
   * A test, comparing the answer of 2,1 with whatever calculateResults produces.
   */
  @Test
  def calculateResultsTest(): Unit = {
    val dataset = TaskTwo.calculateResults(dataFrame)
    val expected = Array[Long](2, 1)
    val actual = dataset.select(TaskTwo.Count).as[Long].collect()
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
