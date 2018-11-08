import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test suite for task3.
 */
class Task3Test extends JUnitSuite {

  /**
   * Spark session necessary for this task.
   */
  private val spark = Task3.buildSession()

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
  ).toDF("srch_adults_cnt", "srch_children_cnt", "is_booking", "hotel_country", "hotel_market", "hotel_continent")

  /**
   * A test, comparing the answer of 3,1,1 with whatever calculateResults produces.
   */
  @Test
  def calculateResultsTest() : Unit = {
    val dataset = Task3.calculateResults(dataFrame)
    val expected = Array[Long](3,1,1)
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
