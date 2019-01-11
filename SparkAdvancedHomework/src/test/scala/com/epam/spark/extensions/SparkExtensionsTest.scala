package com.epam.spark.extensions

import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Test}
import org.scalatest.junit.JUnitSuite

class SparkExtensionsTest extends JUnitSuite {

  import com.epam.spark.extensions.SparkExtensions._

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Test")
    .getOrCreate()

  import spark.implicits._

  @After
  def tearDown(): Unit = {
    spark.stop()
  }

  @Test
  def loadDataFromHDFSTest(): Unit = {

  }

  @Test
  def mergeRunningTotalsTest(): Unit = {


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

    val actualDF = spark.mergeRunningTotals(df1, df2).cache()

    val expected = expectedDF.head()
    val actual = actualDF.head()

    Assert.assertEquals(expected, actual)
  }

  @Test
  def extractIntersectionPathsTest(): Unit = {
    val dataFrame = Seq(
      ("19 JANUARY", "15", "hashTag", "123", 1L),
      ("19 JANUARY", "15", "hashTag", "123", 2L),
      ("19 JANUARY", "15", "hashTag", "123", 3L),
      ("19 JANUARY", "15", "hashTag", "123", 4L)
    ).toDF("date", "hour", "hashTag", "userId", "cnt")

    val actual = spark.extractIntersectionPaths(dataFrame)
    val expected = Array(
      "/date=19 JANUARY/hour=15/hashTag=hashTag/userId=123",
      "/date=19 JANUARY/hour=15/hashTag=hashTag/userId=123",
      "/date=19 JANUARY/hour=15/hashTag=hashTag/userId=123",
      "/date=19 JANUARY/hour=15/hashTag=hashTag/userId=123"
    )

    Assert.assertTrue(expected.sameElements(actual))
  }


}
