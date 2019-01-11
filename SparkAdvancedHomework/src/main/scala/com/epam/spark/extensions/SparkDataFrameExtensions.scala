package com.epam.spark.extensions

import com.epam.spark.extensions.SparkExtensions.TweetSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.implicitConversions

/**
  * A utility class for creating dataframes in spark.
  */
object SparkDataFrameExtensions {

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param spark the sparkSession object being extended.
    * @return RichSparkDataFrame object that extends sparkSession.
    */
  implicit def richSparkDataFrame(spark: SparkSession): RichSparkDataFrame = new RichSparkDataFrame(spark)

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param spark the sparkSession object being extended.
    */
  class RichSparkDataFrame(spark: SparkSession) {
    /**
      * A shortcut method to create an dataframe with twitter schema.
      *
      * @return an empty dataframe with twitter schema.
      */
    def emptyTypedDataFrame: DataFrame = {
      val emptyRDD = spark.emptyDataFrame.rdd
      spark.createDataFrame(emptyRDD, TweetSchema)
    }
  }

}
