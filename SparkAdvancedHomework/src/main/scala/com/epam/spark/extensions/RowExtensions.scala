package com.epam.spark.extensions

import org.apache.spark.sql.Row

import scala.language.implicitConversions

/**
  * A utility class, providing extended functionality to the Row object.
  */
object RowExtensions {
  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param row the row object being extended.
    * @return RichRow object with extensions.
    */
  implicit def richRow(row: Row): RichRow = new RichRow(row)

  /**
    * Pimp my library pattern.
    *
    * @see https://stackoverflow.com/a/3119671/10681828
    * @param row the row object being extended.
    */
  class RichRow(row: Row) {
    /**
      * If we write the Row (key=key1, date=date1, cnt=4) to the HDFS partitioned by key and date.
      * It will make up this path: "/date=date1/key=key1/file_with_cnt.csv".
      * So, this method converts a row to its corresponding path string.
      *
      * @return the corresponding path string.
      */
    def extractValueString: String = {
      val dateColumnId = 0
      val hourColumnId = 1
      val hashTagColumnId = 2
      val userIdColumnId = 3

      val dateString = "/date=" + row.getAs[String](dateColumnId)
      val hourString = "/hour=" + row.getAs[String](hourColumnId)
      val hashTagString = "/hashTag=" + row.getAs[String](hashTagColumnId)
      val userIdString = "/userId=" + row.getAs[String](userIdColumnId)

      dateString + hourString + hashTagString + userIdString
    }
  }

}
