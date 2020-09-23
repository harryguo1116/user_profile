package cn.itcast.tags.test.hbase.condition

import cn.itcast.tags.meta.HBaseMeta
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * @Author Harry
 * @Date 2020-08-27 22:17
 * @Description
 */
object HBaseConditionTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions","4")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // 1. 从HBase表读取数据规则集合Map
    val ruleMap: Map[String, String] = Map(
      "inType"-> "hbase",
      "zkHosts"-> "bigdata-cdh01.itcast.cn",
      "zkPort"-> "2181",
      "hbaseTable"-> "tbl_profile_users",
      "family"-> "detail",
      "selectFieldNames"-> "id,gender",
      "whereCondition"-> "modified#day#30"
    )
    // 2. 规则数据封装到HBaseMeta中
    val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
    // HBaseMeta(bigdata-cdh01.itcast.cn,2181,tbl_profile_users,detail,id,gender,modified[GE]2020-
    //01-17 13:44:29,modified[LE]2020-02-15 13:44:29)
    println(hbaseMeta)
    // 3. SparkSQL从HBase表读取数据
    val usersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", hbaseMeta.zkHosts)
      .option("zkPort", hbaseMeta.zkPort)
      .option("hbaseTable", hbaseMeta.hbaseTable)
      .option("family", hbaseMeta.family)
      .option("selectFields", hbaseMeta.selectFieldNames)
      //.option("filterConditions", hbaseMeta.filterConditions)
      .load()
    usersDF.printSchema()
    usersDF.show(10, truncate = false)
    println(s"count = ${usersDF.count()}")
    // 应用结束，关闭资源
    spark.stop()
  }
}
