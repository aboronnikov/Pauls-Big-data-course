package com.epam.spark.extensions

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
     * Eagerly persists this dataframe to disk memory (as opposed to lazily...).
     * Meaning the dataframe will be written to disk right away.
     * This is used in case you want to overwrite the data that will be in use while overwriting.
     *
     * @return an eagerly persisted DataFrame.
     */
    def persistEagerly: DataFrame = {
      val persisted = df.persist(StorageLevel.DISK_ONLY)
      persisted.count() // count is done here on purpose to make this operation eager.
      persisted
    }
  }

}
