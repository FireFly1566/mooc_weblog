package third_topn

import myutils.{DayVideoAccessStat, StatDAO}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/*
* 对第二次清洗的结果进行统计
* */
object TopNStatJob {

  /*
  * 最受欢迎的 TopN 课程
  * */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    /*
    * DataFrame 方式统计
    * */
    //    import spark.implicits._
    //    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
    //      .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //
    //    videoAccessTopNDF.show(false)

    /*
    * SQL 方式统计
    * */
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day,cmsId, count(1) as times from access_logs" +
      " where day='20170511' and cmsType='video'" +
      " group by day,cmsId order by times desc")

    //videoAccessTopNDF.show(false)

    /*
    * 将统计结果写入到数据库
    * */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))

        })

        StatDAO.insertDayVideoAccessStat(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }

  def main(args: Array[String]): Unit = {

    /*
    * 默认会将 day 推断为 integer类型，https://spark.apache.org/docs/2.1.3/sql-programming-guide.html#programmatically-specifying-the-schema
    *
    * */
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("D://weblog//out1")

    //    accessDF.printSchema()
    //    accessDF.show(false)

    /*
    * 最受欢迎的 TopN 视频课程
    * */
    videoAccessTopNStat(spark, accessDF)


    spark.stop()
  }
}
