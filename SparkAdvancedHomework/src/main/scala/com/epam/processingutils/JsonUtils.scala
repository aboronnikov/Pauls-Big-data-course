package com.epam.processingutils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.function.Function

import com.epam.twittertrivia.TweetWithKeys
import com.jayway.jsonpath.JsonPath

/**
 * This class provides json utilities, to extract certain pieces of information from a tweet's json.
 */
object JsonUtils {

  /**
   * Extracts hash tags from a tweet's json.
   *
   * @param tweetJson json string with tweet info.
   * @return returns a list of hashtags.
   */
  private def extractHashtags(tweetJson: String): java.util.List[String] = {
    JsonPath.parse(tweetJson).read("$.entities.hashtags[*].text")
  }

  /**
   * Extracts a data from a tweet's json.
   *
   * @param tweetJson json string with tweet info.
   * @return date string.
   */
  private def extractDate(tweetJson: String): String = {
    JsonPath.parse(tweetJson).read("$.created_at")
  }

  private def extractUserId(tweetJson: String): String = {
    JsonPath.parse(tweetJson).read("$.user.id_str")
  }

  /**
   * A simplified tweet object, stripped to only include the necessary information.
   *
   * @param tweetJson json string with tweet info.
   * @return returns an object with stripped tweet information.
   */
  def transformTweetStringIntoObject(tweetJson: String): List[TweetWithKeys] = {
    import DateTimeExtensions._

    import scala.collection.JavaConverters._
    import scala.compat.java8.FunctionConverters._

    val dateTimeStr = JsonUtils.extractDate(tweetJson)
    val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss ZZZ yyyy")
    val dateTime = LocalDateTime.parse(dateTimeStr, formatter)

    val hashTags = JsonUtils.extractHashtags(tweetJson)

    val userId = JsonUtils.extractUserId(tweetJson)

    hashTags.stream()
      .map[TweetWithKeys](((hashTag: String) => TweetWithKeys(dateTime.getDateStr(), dateTime.getHourStr(), hashTag, userId)).asJava)
      .iterator()
      .asScala
      .toList
  }

  def funToFunction[InT, OutT](fun: InT => OutT): Function[InT, OutT] = new Function[InT, OutT] {
    override def apply(t: InT): OutT = fun(t)
  }
}
