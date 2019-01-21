package com.epam.spark.extensions

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Tests the SparkExtensions class.
 */
class SparkExtensionsTest extends JUnitSuite {

  import com.epam.spark.extensions.SparkExtensions._

  /**
   * Tests the mergeRunningTotals method, making sure that it merges our results properly.
   */
  @Test
  def mergeRunningTotalsTest(): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    import spark.implicits._

    val df1 = Seq(
      ("19 JANUARY", "15", "hashTag", "123", 4L)
    ).toDF("date", "hour", "hashTag", "userId", "cnt")

    val df2 = Seq(
      ("19 JANUARY", "15", "hashTag", "123", 6L)
    ).toDF("date", "hour", "hashTag", "userId", "cnt")

    val expectedDF = spark.sparkContext.parallelize(
      Seq(("19 JANUARY", "15", "hashTag", "123", 10L))
    ).toDF("date", "hour", "hashTag", "userId", "cnt")
      .cache()

    val actualDF = spark.mergeRunningTotals(df1, df2, Array("date", "hour", "hashTag", "userId"), "cnt").cache()

    val expected = expectedDF.head()
    val actual = actualDF.head()

    Assert.assertEquals(expected, actual)
  }

  /**
   * Tests the emptyRDD method, making sure that it returns an empty RDD with the necessary schema.
   */
  @Test
  def emptyRDDTest(): Unit = {
    import com.epam.spark.extensions.SparkExtensions._
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val emptyDF = spark.emptyTypedDataFrame(program.TopicDataIngester.TweetSchema)
    val expectedSchema = program.TopicDataIngester.TweetSchema
    val actualSchema = emptyDF.schema

    Assert.assertEquals(expectedSchema, actualSchema)

    val expectedCount = 0
    val actualCount = emptyDF.count()

    Assert.assertEquals(expectedCount, actualCount)
  }
}
