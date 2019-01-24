import com.epam.utilities.{JsonUtils, TweetInfo}
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
  * Tests for the JsonUtils class.
  */
class JsonUtilsTest extends JUnitSuite {

  /**
    * Tests the transformTweetStringIntoObjects method.
    */
  @Test
  def transformTweetStringIntoObjectsTest(): Unit = {
    val exampleTweet =
      """{
      "created_at": "Wed Jan 16 16:08:59 +0000 2019",
      "user": {
        "id_str":"12342142"
      },
      "entities": {
        "hashtags": [
      {"text":"Pavel1"},
      {"text":"Pavel2"},
      {"text":"Pavel3"}
        ]
      }
    }"""

    val actualListOfTweets = JsonUtils.transformTweetStringIntoObjects(exampleTweet)

    val expectedListOfTweets = List(
      TweetInfo("2019-01-16T16:08:59", "Pavel1", "12342142"),
      TweetInfo("2019-01-16T16:08:59", "Pavel2", "12342142"),
      TweetInfo("2019-01-16T16:08:59", "Pavel3", "12342142")
    )

    Assert.assertEquals(expectedListOfTweets, actualListOfTweets)

  }
}
