package second_analyze

import org.apache.spark.sql.SparkSession

import myutils.AccessConvertUtil

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///access.log")

    //accessRDD.take(10).foreach(println)
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)

    spark.stop()

  }
}
