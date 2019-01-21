package com.epam.processingutils

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Tests for the DataFrameUtils class
 */
class DataFrameUtilsTest extends JUnitSuite {
  /**
   * Tests that extractIntersectionPaths extracts the paths from row objects' data properly.
   */
  @Test
  def extractIntersectionPathsTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    import spark.implicits._

    val dataFrame = Seq(
      ("19 JANUARY", "15", "hashTag", "123", 1L),
      ("19 JANUARY", "15", "hashTag", "123", 2L),
      ("19 JANUARY", "15", "hashTag", "123", 3L),
      ("19 JANUARY", "15", "hashTag", "123", 4L)
    ).toDF("date", "hour", "hashTag", "userId", "cnt")

    val actual = DataFrameUtils.extractIntersectionPaths(dataFrame)
    val expected = Array(
      "/date=19 JANUARY/hour=15"
    )

    Assert.assertTrue(expected.sameElements(actual))
  }
}
