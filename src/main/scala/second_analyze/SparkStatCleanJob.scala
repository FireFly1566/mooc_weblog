package second_analyze

import org.apache.spark.sql.{SaveMode, SparkSession}
import myutils.AccessConvertUtil

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///access.log")

    //accessRDD.take(10).foreach(println)
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //    accessDF.printSchema()
    //    accessDF.show(false)

    //将数据按照天输出
    /*
    * 调优点：通过 coalesce控制文件输出的大小
    * */
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("D://weblog//out1")

    spark.stop()

  }
}
