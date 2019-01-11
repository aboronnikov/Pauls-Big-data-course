package com.epam.processingutils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

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
  private def extractHashtags(tweetJson: String): List[String] = {
    val hashTags = JsonPath.parse(tweetJson).read[java.util.List[String]]("$.entities.hashtags[*].text")
    getScalaList(hashTags)
  }

  /**
    * Extracts a data from a tweet's json.
    *
    * @param tweetJson json string with tweet info.
    * @return date string.
    */
  private def extractDate(tweetJson: String): LocalDateTime = {
    val dateTimeStr = JsonPath.parse(tweetJson).read[String]("$.created_at")
    val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss ZZZ yyyy")
    LocalDateTime.parse(dateTimeStr, formatter)
  }

  private def extractUserId(tweetJson: String): String = {
    JsonPath.parse(tweetJson).read("$.user.id_str")
  }

  /**
    * Helper class that converts a java.util.List to a scala List.
    *
    * @param list java.util.List to be converted.
    * @return a scala List.
    */
  private def getScalaList(list: java.util.List[String]): List[String] = {
    import scala.collection.JavaConverters._
    list.stream()
      .iterator()
      .asScala
      .toList
  }

  /**
    * A simplified tweet object, stripped to only include the necessary information.
    *
    * @param tweetJson json string with tweet info.
    * @return returns an object with stripped tweet information.
    */
  def transformTweetStringIntoObjects(tweetJson: String): List[TweetWithKeys] = {
    import DateTimeExtensions._

    val dateTime = extractDate(tweetJson)
    val hashTags = extractHashtags(tweetJson)
    val userId = extractUserId(tweetJson)

    val date = dateTime.getDateStr()
    val hour = dateTime.getHourStr()

    hashTags
      .map(hashTag => TweetWithKeys(date, hour, hashTag, userId))
  }
}
