package com.epam.spark.extensions

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions

/**
  * A utility class that enriches DataFrame with extra functionality.
  */
object DataFrameExtensions {
  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param df the dataframe being extended
    * @return RichDataFrame object with extensions.
    */
  implicit def richDataFrame(df: DataFrame): RichDataFrame = new RichDataFrame(df)

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param df the dataframe being extended.
    */
  class RichDataFrame(df: DataFrame) {
    /**
      * Makes a dataframe union compatible. Otherwise the union operation won't work properly.
      * More on this here:
      *
      * @see https://stackoverflow.com/questions/32705056/what-is-going-wrong-with-unionall-of-spark-dataframe
      * @return a union compatible dataframe.
      */
    def unionCompatible: DataFrame = df.select("date", "hour", "hashTag", "userId", "cnt")

    /**
      * Eagerly persists this dataframe to disk memory (as opposed to lazily...).
      * Meaning the dataframe will be written to disk right away.
      * This is used in case you want to overwrite the data that will be in use while overwriting.
      *
      * @return an eagerly persisted dataframe.
      */
    def persistEagerly: DataFrame = {
      val persisted = df.persist(StorageLevel.DISK_ONLY)
      persisted.count() // count is done here on purpose to make this operation eager.
      persisted
    }
  }

}
