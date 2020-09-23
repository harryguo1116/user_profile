package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-24 21:36
 * @Description
 */
object JobModel extends Logging {
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
        // TODO：与Hive集成，读取Hive表的数据
        .enableHiveSupport()
        .config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
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
    //标签数据会多次使用 进行缓存
    basicTagDF.persist(StorageLevel.MEMORY_ONLY_2).count()
    // TODO: 3. 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
    //3.1 获取业务标签数据的标签规则 rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4) //四级标签里有rule的介绍
      .head() //获取第一条数据
      .getAs[String]("rule")
    //logWarning(s"==========${tagRule}==========")
    /**
     * inType=hbase
     * zkHosts=bigdata-cdh01.itcast.cn
     * zkPort=2181
     * hbaseTable=tbl_tag_users
     * family=detail
     * selectFieldNames=id,job
     */
    //3.2 解析规则 存储至Map集合
    val ruleMap: Map[String, String] = tagRule
      .split("\\n")
      .map { line =>
        val Array(attrKey, attrValue) = line.trim.split("=")
        attrKey -> attrValue
      }.toMap
    //inType -> hbase,zkHosts -> bigdata-cdh01.itcast.cn,
    // zkPort -> 2181,hbaseTable -> tbl_tag_users,
    // selectFieldNames -> id,job,family -> detail
    //logWarning(s"==========${ruleMap.mkString(",")}======")

    //3.3 判断数据源 依据数据源获取业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase)) {
      // 将Map集合中数据源信息封装到Meta表中
      val hBaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      //从HBase表中加载业务数据
      businessDF = HBaseTools.read(
        spark, hBaseMeta.zkHosts, hBaseMeta.zkPort,
        hBaseMeta.hbaseTable, hBaseMeta.family,
        hBaseMeta.selectFieldNames.split(",")
      )
    } else {
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法进行标签计算")
    }
    //businessDF.show(10)
    //businessDF.printSchema()
    /**
     * root
     * |-- id: string (nullable = true)
     * |-- job: string (nullable = true)
     */
    //todo 4 业务数据和属性标签结合 构建标签：规则匹配型标签 -> rule match
    val attrTagMap: Map[String, Long] = basicTagDF
      .filter($"level" === 5)
      .select(
        $"rule",
        $"id".as("tagId")
      )
      .as[(String, Long)]//转为DataSet进行操作
      .rdd
      .collectAsMap().toMap
    //使用广播变量 提高性能
    val broadcastMap: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(attrTagMap)
    //3.2 自定义udf函数
    val job_to_tag: UserDefinedFunction = udf(
      (job: String) => broadcastMap.value(job)
    )
    //3.3 使用udf函数 给每个用户打上标签的值 tagId
    val modelDF: DataFrame = businessDF.select(
      $"id".as("uid"),
      job_to_tag($"job").cast(StringType).as("tagId")
    )

    /**
     * root
     * |-- uid: string (nullable = true)
     * |-- tagId: string (nullable = true)
     */
    //modelDF.printSchema()
    //modelDF.show()
    //数据不再使用时 释放缓存
    basicTagDF.unpersist()

    //todo：5 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile
    //5.1 获取HBase数据库中历史画像标签表的数据 profileDF -> tbl_profile
    val profileDF: DataFrame = HBaseTools.read(
      spark, "bigdata-cdh01.itcast.cn", "2181",
      "tbl_profile", "user", Seq("userId", "tagIds")
    )
    //5.2 合并当前数据和历史画像标签数据  newProfileDF
    //a. 按照用户ID关联数据 使用左外连接
    val mergeDF: DataFrame = modelDF.join(
      profileDF, modelDF("uid") === profileDF("userId"), "left"
    )
    //b. 自定义 UDF函数 合并标签
    val merge_tags_udf: UserDefinedFunction = udf(
      (tagId: String, tagids: String) => {
        tagids.split(",").:+(tagId).distinct.mkString(",")
      }
    )
    //c. 获取最新用户画像标签数据
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"),
      when($"tagIds".isNull, $"tagId")
        .otherwise(merge_tags_udf($"tagId", $"tagIds"))
        .as("tagIds")
    )
    //newProfileDF.printSchema()
    //newProfileDF.show()
    //5.3 把最新的用户画像标签保存至HBase
    HBaseTools.write(
      newProfileDF,"bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId"
    )
    //关闭资源
    spark.stop()
  }
}
