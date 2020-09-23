package cn.itcast.tags.test.profile
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author Harry
 * @Date 2020-09-01 14:55
 * @Description  保存画像标签数据到HBase表tbl_profile中，加载JSON格式数据
 */
object SaveProfileTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    // 加载画像标签数据
    val profileDF: DataFrame = spark.read
      .json("datas/profile-tags/tbl_profile.json")
    profileDF.show(10, truncate = false)
    // 保存数据至HBase表中
    profileDF.write
      .mode(SaveMode.Overwrite)
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_profile")
      .option("family", "user")
      .option("rowKeyColumn", "userId")
      .save()
    // 应用结束，关闭资源
    spark.stop()

  }

}
