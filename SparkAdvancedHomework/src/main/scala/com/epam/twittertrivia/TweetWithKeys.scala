package com.epam.twittertrivia

/**
 * This is a simplified representation of a tweet.
 *
 * @param date    date data string
 * @param hour    hour data string
 * @param hashTag hash tag data string
 * @param userId  user id data string
 */
final case class TweetWithKeys(date: String, hour: String, hashTag: String, userId: String)
