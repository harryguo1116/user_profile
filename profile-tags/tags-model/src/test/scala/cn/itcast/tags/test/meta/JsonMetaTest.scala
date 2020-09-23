package cn.itcast.tags.test.meta

import cn.itcast.tags.meta.MetaParse
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonMetaTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 1. 参数Map集合
    val paramsMap: Map[String, String] = Map(
      "inType" -> "json",
      "inPath" -> "datas\\profile-tags\\tbl_profile.json",
      "sperator" -> ",",
      "selectFieldNames" -> "userId,tagIds"
    )

    // 2. 加载数据
    val dataframe: DataFrame = MetaParse.parseMetaToData(spark, paramsMap)

    dataframe.printSchema()
    dataframe.show(20, truncate = false)

    spark.stop()
  }
}