package myutils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/*
*时间解析工具类
* [10/Nov/2016:00:01:02 +0800]
* */
object DateUtils {

  /*
  * SimpleDateFormat 是线程不安全的
  * */

  //10/Nov/2016:00:01:02 +0800,输入的日期格式
  //val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //输出的日期格式
  //val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /*
  * 获取输入的日志时间
  * time : [10/Nov/2016:00:01:02 +0800]
  * */
  def getTime(time: String) = {

    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }

}
