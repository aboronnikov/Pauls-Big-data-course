package com.epam.processingutils

import org.apache.spark.sql.DataFrame
import program.TopicDataIngester.{Date, Hour}

/**
 * Utility class for working with DataFrames.
 */
object DataFrameUtils {

  /**
   * Extract paths that we will need to use to load data from the HDFS.
   * This method is written to be able to identify what existing partitions will be in use when
   * merging with new data from kafka.
   *
   * @param df the DataFrame just read from kafka.
   * @return an array on paths.
   */
  def extractIntersectionPaths(df: DataFrame): Array[String] = {
    df
      .select(Date, Hour) // path values
      .rdd
      .map(RowUtils.extractValueString)
      .distinct()
      .collect() // this inexpensive collect here is done on purpose, to avoid the RDD[RDD] scenario later on.
  }
}
