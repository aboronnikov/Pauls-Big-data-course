package com.epam.processingutils

/**
 * Tests the JsonUtils class.
 */
class JsonUtilsTest extends JUnitSuite {

  /**
   * Tests the transformTweetStringIntoObjects method, makes sure that everything is properly parsed.
   * For more info refer to the method itself, it's got an explanation of how it works there.
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
      TweetWithKeys("16 JANUARY", "16", "Pavel1", "12342142"),
      TweetWithKeys("16 JANUARY", "16", "Pavel2", "12342142"),
      TweetWithKeys("16 JANUARY", "16", "Pavel3", "12342142")
    )

    Assert.assertEquals(expectedListOfTweets, actualListOfTweets)
  }
}
