package com.epam.processingutils

import org.apache.spark.sql.Row

/**
 * A utility class, providing extended functionality to the Row object.
 */
object RowUtils {
  /**
   * If we write the Row (key=key1, date=date1, cnt=4) to the HDFS partitioned by key and date.
   * It will make up this path: "/date=date1/key=key1/file_with_cnt.csv".
   * So, this method converts a row to its corresponding path string.
   *
   * @return the corresponding path string.
   */
  def extractValueString(row: Row): String = {
    val dateColumnId = 0
    val hourColumnId = 1

    val dateString = "/date=" + row.getAs[String](dateColumnId)
    val hourString = "/hour=" + row.getAs[String](hourColumnId)

    dateString + hourString
  }
}
