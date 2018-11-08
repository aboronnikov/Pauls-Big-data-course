import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test suite for task1.
 */
class Task1Test extends JUnitSuite {

  /**
   * Spark session necessary for this task.
   */
  private val spark = Task1.buildSession()

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
  ).toDF("srch_adults_cnt", "hotel_country", "hotel_market", "hotel_continent")

  /**
   * A test, comparing the answer of 2,1,1,1 with whatever calculateResults produces.
   */
  @Test
  def calculateResultsTest(): Unit = {
    val dataset = Task1.calculateResults(dataFrame)
    val expected = Array[Long](2,1,1,1)
    val actual = dataset.select("count").as[Long].collect()
    Assert.assertArrayEquals(expected, actual)
  }

  /**
   * This is a necessary procedure of stopping spark session to clean up resources.
   */
  @After
  def tearDown() : Unit = {
    spark.stop()
  }
}
