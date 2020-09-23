package cn.itcast.tags.test.order

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-26 21:14
 * @Description   读取HBase表中订单表的数据，指定字段信息
 */
object HBaseOrderTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 读取数据
    val ordersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "memberid,finishtime")
      .load()
    ordersDF.printSchema()
    ordersDF.persist(StorageLevel.MEMORY_AND_DISK)
    ordersDF.show(50, truncate = false)
    val userCount: Long = ordersDF
      // 去重统计会员人数
      .agg(
        countDistinct($"memberid").as("total")
      )
      .head()
      .getAs[Long]("total")
    println(s"userCount = $userCount")
    spark.stop()

  }
}
