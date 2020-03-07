package util

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap

import scala.util.Try

/**
 * Created by shaofengfeng on 2019/10/09.
 */
object DateTimeUtil {
  private val formatters = new ConcurrentHashMap[String, DateTimeFormatter]

  import scala.collection.JavaConversions._

  private def f(format: String): DateTimeFormatter = formatters.getOrElseUpdate(format, DateTimeFormatter.ofPattern(format))

  def currentDateTimeStr(format: String = "yyyyMMddHHmmss"): String = f(format).format(LocalDateTime.now())

  def currentDateStr(format: String = "yyyyMMdd"): String = f(format).format(LocalDateTime.now())

  def beforeCurrentDateStr(offset: Int, format: String = "yyyyMMdd"): String = {
    val ld = LocalDateTime.now().minusDays(offset)
    f(format).format(ld)
  }

  def afterCurrentDateStr(offset: Int, format: String = "yyyyMMdd"): String = {
    val ld = LocalDateTime.now().plusDays(offset)
    f(format).format(ld)
  }

  def offsetDateStr(dateStr:String,offset: Int, format: String = "yyyyMMdd"): String = {
    val ld=LocalDate.parse(dateStr,f(format))
    //val resLd= if(offset>=0) ld.plusDays(offset) else ld.minusDays(offset)
    val resLd= ld.plusDays(offset)
    f(format).format(resLd)
  }

  def dateTime2millis(dateTimeStr: String, format: String = "yyyyMMddHHmmss"): Option[Long] = {
    import java.time.ZoneId
    Try {
      val localDateTime = LocalDateTime.parse(dateTimeStr, f(format))
      Some(localDateTime.atZone(ZoneId.systemDefault).toInstant.toEpochMilli)
    }.getOrElse(None)
  }

  def date2millis(dateTimeStr: String, format: String = "yyyyMMdd"): Option[Long] = {
    import java.time.ZoneId
    Try {
      val localDate = LocalDate.parse(dateTimeStr, f(format))
      Some(LocalDateTime.of(localDate,LocalTime.of(0,0,0))
        .atZone(ZoneId.systemDefault).toInstant.toEpochMilli)
    }.getOrElse(None)
  }

  def millis2datetime(ts:Long): LocalDateTime = {
    LocalDateTime.ofEpochSecond(ts/1000,0,ZoneOffset.ofHours(8))
  }

  def yyyyMMddHHmmss2millis(timeStr: String): Option[Long] = {
    dateTime2millis(timeStr)
  }

  def main(args: Array[String]): Unit = {
    //    println(offsetDateStr("20191225",-60))
    println(afterCurrentDateStr(-2))
  }
}
