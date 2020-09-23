package cn.itcast.tags.models

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-25 18:12
 * @Description 标签基类，各个标签模型继承此类，实现其中打标签方法doTag即可
 */
trait BasicModel extends Logging {

  //变量申明
  var spark: SparkSession = _

  //1、 初始化  构建SparkSession实例对象
  def init() = {
    // a. 创建SparkConf,设置应用相关配置
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      // 设置序列化为：Kryo
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable],
        classOf[Result], classOf[Put]))
      // 设置Shuffle分区数目
      .set("spark.sql.shuffle.partitions", "4")
      // 设置与Hive集成: 读取Hive元数据MetaStore服务
      .set("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
      // 设置数据仓库目录
      .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    // b. 建造者模式创建SparkSession会话实例对象
    spark = SparkSession.builder()
      .config(sparkConf)
      // 启用与Hive集成
      .enableHiveSupport()
      .getOrCreate()
  }

  //2、准备标签数据 依据标签ID从MySQL数据库表tbl_basic_tag标签数据
  def getTagData(tagId: Long): DataFrame = {
    // 2.a. 标签查询语句
    val tagTable =
      s"""
         |(
         |SELECT `id`,
         | `name`,
         | `rule`,
         | `level`
         |FROM `profile_tags`.`tbl_basic_tag`
         |WHERE id = $tagId
         |UNION
         |SELECT `id`,
         | `name`,
         | `rule`,
         | `level`
         |FROM `profile_tags`.`tbl_basic_tag`
         |WHERE pid = $tagId
         |ORDER BY `level` ASC, `id` ASC
         |) AS basic_tag
         |""".stripMargin
    //2.b 获取标签数据
    val tagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url",
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
          "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    //2.c 返回数据
    tagDF
  }

  //3、 业务数据 依据业务标签规则rule 从数据源获取业务数据
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    import tagDF.sparkSession.implicits._

    //3.1 获取4级标签的rule
    val tagRule: String = tagDF
      .filter($"level" === 4) //四级标签里有rule的介绍
      .head() //获取第一条数据
      .getAs[String]("rule")
    //3.2 解析标签规则 先按照换行符"\n"分割 再按照等号分割
    val ruleMap: Map[String, String] = tagRule
      .split("\\n")
      .map { line =>
        val Array(attrKey, attrValue) = line.trim.split("=")
        attrKey -> attrValue
      }.toMap
    //3.3 依据标签规则中 inType类型获取数据源
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
    businessDF
  }

  //4、 构建标签 依据业务数据和属性标签数据建立标签
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  //5、标签合并与保存 读取用户标签数据 进行合并操作 最后保存
  def mergeAndSaveTag(modelDF: DataFrame): Unit = {
    import cn.itcast.tags.tools.ProfileTools
    //a. 从HBase表中读取画像标签表数据
    val profileDF: DataFrame = ProfileTools.loadProfile(spark)
    //b. 将用户标签数据合并
    val newProfileDF: DataFrame = ProfileTools.mergeProfileTags(modelDF, profileDF)
    //c. 保存至HBase表中
    ProfileTools.saveProfile(newProfileDF)
  }

  //6、 关闭资源 应用结束 关闭会话实例对象
  def close() = {
    if (null != spark) spark.stop()
  }

  //规定标签模型执行流程顺序
  def executeModel(tagId: Long) = {
    //a. 初始化
    init()
    try {
      //b. 获取标签数据
      val tagDF: DataFrame = getTagData(tagId)
      println("========tagDF=========")
      tagDF.show()
      tagDF.persist(StorageLevel.MEMORY_AND_DISK).count()
      //c. 获取业务数据
      val businessDF: DataFrame = getBusinessData(tagDF)
      println("========businessDF=========")
      businessDF.show()
      //d. 计算标签
      val modelDF: DataFrame = doTag(businessDF, tagDF)
      println("========modelDF=========")
      modelDF.show()
      //e. 合并标签与保存
      mergeAndSaveTag(modelDF)
      println("save  success  . . . ")
      tagDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close()
    }
  }
}
