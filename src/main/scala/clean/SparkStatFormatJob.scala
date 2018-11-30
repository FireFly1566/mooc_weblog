package clean

import org.apache.spark.sql.SparkSession


/*
* 第一步清洗：抽取需要的指定列的数据
* */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///access_20000.log")

    access.take(10).foreach(println)


    spark.stop()

  }

}
