package third_topn

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._

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

    videoAccessTopNDF.show(false)
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
