package com.epam.processingutils

import java.time.LocalDateTime

import scala.language.implicitConversions

/**
 * Extension class for LocalDateTime.
 */
object DateTimeExtensions {
  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param localDateTime This LocalDateTime instance.
   * @return RichLocalDateTime instance.
   */
  implicit def richLocalDateTime(localDateTime: LocalDateTime): RichLocalDateTime = new RichLocalDateTime(localDateTime)

  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param dateTime This LocalDateTime.
   */
  class RichLocalDateTime(dateTime: LocalDateTime) {

    /**
     * Convenience method for extracting hour as a string.
     *
     * @return string with the hour value.
     */
    def getHourStr: String = {
      dateTime.getHour.toString
    }

    /**
     * Convenience method for extracting date as a string.
     *
     * @return string with the date value.
     */
    def getDateStr: String = {
      dateTime.getDayOfMonth + " " + dateTime.getMonth.name()
    }
  }

}
