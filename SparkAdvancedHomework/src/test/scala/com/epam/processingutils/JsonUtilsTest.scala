package com.epam.processingutils

import com.epam.twittertrivia.TweetWithKeys
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class JsonUtilsTest extends JUnitSuite {

  @Test
  def transformTweetStringIntoObjectsTest(): Unit = {
    val exampleTweet = """{
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
