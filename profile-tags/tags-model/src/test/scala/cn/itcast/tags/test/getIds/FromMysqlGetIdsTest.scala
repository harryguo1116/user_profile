package cn.itcast.tags.test.getIds

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @Author Harry
 * @Date 2020-08-27 09:16
 * @Description
 */
object FromMysqlGetIdsTest {
  def main(args: Array[String]): Unit = {
    /*
   321	职业
     322	学生		1
     323	公务员	2
     324	军人		3
     325	警察		4
     326	教师		5
     327	白领		6
    */
    // 获取SparkSession对象
    val spark: SparkSession = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[4]")
        .set("spark.sql.shuffle.partitions", "4")
        // 由于从HBase表读写数据，设置序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put])
        )
      val session: SparkSession = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
      session
    }
    import spark.implicits._

    //todo： 2 从MySQL数据库读取标签数据(基础标签表：tag_basic_tag)，依据业务标签ID读取
    //根据标签ID：tagID(4级标签、业务标签)，查询标签相关数据
    val tagTable: String =
    """
      |(
      |SELECT `id`,
      |       `name`,
      |       `rule`,
      |       `level`
      |FROM `profile_tags`.`tbl_basic_tag`
      |WHERE id = 321
      |UNION
      |SELECT `id`,
      |       `name`,
      |       `rule`,
      |       `level`
      |FROM `profile_tags`.`tbl_basic_tag`
      |WHERE pid = 321
      |ORDER BY `level` ASC, `id` ASC
      |) AS basic_tag
      |""".stripMargin

    //使用SparkSQL内置JDBC方式读取MySQL表数据
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url",
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
          "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()

    val ids: Set[String] = basicTagDF
      // 获取属性标签数据
      .filter($"level" === 5)
      .rdd // 转换为RDD
      .map { row => row.getAs[Long]("id") }
      .collect()
      .map(_.toString)
      .toSet

    println(ids)
  }

}
