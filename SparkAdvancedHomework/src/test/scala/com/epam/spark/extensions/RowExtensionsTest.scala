package com.epam.spark.extensions

import org.apache.spark.sql.Row
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class RowExtensionsTest extends JUnitSuite {
  @Test
  def extractValueStringTest() : Unit = {
    import com.epam.spark.extensions.RowExtensions._
    val row = Row("16 JANUARY", "16", "hashtag", "12345")
    val expectedValueString = row.extractValueString
    val actualValueString = "/date=16 JANUARY/hour=16/hashTag=hashtag/userId=12345"
    Assert.assertEquals(expectedValueString, actualValueString)
  }
}
