package first_clean

import myutils.DateUtils
import org.apache.spark.sql.SparkSession


/*
* 第一步清洗：抽取需要的指定列的数据
* */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///access_20000.log")

    //access.take(10).foreach(println)

    /*
    * 原始日志的第3和4个字段拼接起来就是时间
    * (183.162.52.7,[10/Nov/2016:00:01:02 +0800])
    * 要将时间转换为 yyyy-MM-dd HH:mm:ss
    * */
    /*access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)

      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).take(10).foreach(println)*/

    access.map(line => {
     val splits = line.split(" ")
     val ip = splits(0)
     val time = splits(3) + " " + splits(4)
     val url = splits(11).replaceAll("\"", "")
     val traffic = splits(9)

     DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
   }).saveAsTextFile("file:///D://outlog")


    spark.stop()

  }

}
