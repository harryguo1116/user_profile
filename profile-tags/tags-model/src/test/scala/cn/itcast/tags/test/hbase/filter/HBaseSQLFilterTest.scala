package cn.itcast.tags.test.hbase.filter

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author Harry
 * @Date 2020-08-27 21:42
 * @Description
 */
object HBaseSQLFilterTest {
  def main(args: Array[String]): Unit = {
    //构建spark对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //读取hbase表数据 不设置条件
    import cn.itcast.tags.spark.sql._
    val ordersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "id,memberid,orderamount")
      .load()
    //ordersDF.printSchema()
    //ordersDF.show(50, truncate = false)
    println(s"count = ${ordersDF.count()}")

    // 读取HBase表数据，设置条件: 2019-09-01
    import cn.itcast.tags.spark.sql._
    val dataframe: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "id,memberid,orderamount")
      .option("filterConditions", "modified[lt]2019-09-01")
      .load()
    dataframe.show(50, truncate = false)
    println(s"count = ${dataframe.count()}")

    spark.stop()

  }
}
