package com.epam.processingutils

import java.time.LocalDateTime

import scala.language.implicitConversions

object DateTimeExtensions {
  implicit def richLocalDateTime(localDateTime: LocalDateTime): RichLocalDateTime = new RichLocalDateTime(localDateTime)

  class RichLocalDateTime(dateTime: LocalDateTime) {
    def getHourStr(): String = {
      dateTime.getHour.toString
    }

    def getDateStr(): String = {
      dateTime.getDayOfMonth + " " + dateTime.getMonth.name()
    }
  }

}
