package com.epam.spark.extensions

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class SparkDataFrameExtensionsTest extends JUnitSuite {
  @Test
  def emptyRDDTest() : Unit = {
    import com.epam.spark.extensions.SparkDataFrameExtensions._
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val emptyDF = spark.emptyTypedDataFrame
    val expectedSchema = com.epam.spark.extensions.SparkExtensions.TweetSchema
    val actualSchema = emptyDF.schema

    Assert.assertEquals(expectedSchema, actualSchema)

    val expectedCount = 0
    val actualCount = emptyDF.count()

    Assert.assertEquals(expectedCount, actualCount)

    spark.stop()
  }
}
