package com.epam.utilities

/**
  * Class used as intermediate storage for pertinent tweet data.
  *
  * @param dateTime date and time of tweet's creation
  * @param hashTag  hash tag of the tweet
  * @param userId   user id
  */
final case class TweetInfo(dateTime: String, hashTag: String, userId: String)
