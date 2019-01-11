package com.epam.twittertrivia

/**
 * This is a simplified representation of a tweet.
 *
 * @param tweet the tweet itself.
 * @param date  date string
 * @param key   key string, the key is hashtags appended.
 */
final case class TweetWithKeys(date: String, hour: String, hashTag: String, userId: String)
