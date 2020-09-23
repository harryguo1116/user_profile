package cn.itcast.tags.utils


import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * @Author Harry
 * @Date 2020-08-27 22:00
 * @Description  日期时间工具类
 */
object DateUtils {

  // 格式 ： 年-月-日 时：分：秒
  val LONG_DATE_FORMAT:String = "yyyy-MM-dd HH:mm:ss"
  // 格式 ： 年-月-日
  val SHORT_DATE_FORMAT:String = "yyyy-MM-dd"

  def getNow(format:String = SHORT_DATE_FORMAT):String = {
    //获取当前日期时间
    val today = Calendar.getInstance()
    //返回字符串
    dateToString(today.getTime,format)
  }

  def dateCalculate(dateStr:String,amount:Int,
                    format: String=SHORT_DATE_FORMAT):String = {
    //a. 将日期字符串转换为Date类型
    val date = stringToDate(dateStr,format)
    //b. 获取Calendar对象
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    //c. 设置天数(增加或减少)
    calendar.add(Calendar.DAY_OF_YEAR,amount)
    //d. 转换Date为字符串
    dateToString(calendar.getTime,format)
  }

  /**
   * 把日期转换为字符串
   * @param date   日期
   * @param format 格式
   * @return
   */
  def dateToString(date: Date, format: String): String = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.format(date)
  }

  /**
   * 把日期格式的字符串转换为日期Date类型
   * @param dateStr 日期格式字符串
   * @param format  格式
   * @return
   */
  def stringToDate(dateStr: String, format: String): Date = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.parse(dateStr)
  }

  def main(args: Array[String]): Unit = {
    // 当前日期
    val nowDate: String = getNow()
    println(nowDate)

    // 前一天日期
    val weekDate = dateCalculate(nowDate, -1)
    println(weekDate)

    // 一月后日期
    val weekAfterDate = dateCalculate(weekDate, - 30)
    println(weekAfterDate)
  }

}
