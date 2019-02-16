package com.epam.processingutils

import java.time.LocalDateTime

/**
 * Tests LocalDateTime extensions.
 */
class DateTimeExtensionsTest extends JUnitSuite {

  /**
   * Tests the getHourStr method, which is pretty self explanatory.
   */
  @Test
  def getHourStrTest(): Unit = {
    val dateTime = LocalDateTime.of(1990, 12, 20, 23, 23, 23)
    val expected = "23"
    val actual = dateTime.getHourStr
    Assert.assertEquals(expected, actual)
  }

  /**
   * Tests the getHourStr method, which is pretty self explanatory also.
   */
  @Test
  def getDateStrTest(): Unit = {
    val dateTime = LocalDateTime.of(1990, 12, 20, 23, 23, 23)
    val expected = "20 DECEMBER"
    val actual = dateTime.getDateStr
    Assert.assertEquals(expected, actual)
  }
}
