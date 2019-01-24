package com.epam.utilities

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Utilities for working with dataframes in this program.
  */
object DataFrameUtils {
  /**
    * Aggregates hourly entry counts with hourly watermarking.
    *
    * @param spark          spark session to be used.
    * @param tweetDataTable dataframe with tweet data.
    * @return a dataframe with aggregated hourly counts.
    */
  def aggregateHourlyCountsWithWatermarking(spark: SparkSession, tweetDataTable: DataFrame): DataFrame = {
    import spark.implicits._
    val hourlyDelay = "1 hour"
    tweetDataTable
      .withWatermark("dateTime", hourlyDelay)
      .groupBy(
        window($"dateTime", hourlyDelay, hourlyDelay),
        $"hashTag",
        $"userId"
      ).count()
  }
}
