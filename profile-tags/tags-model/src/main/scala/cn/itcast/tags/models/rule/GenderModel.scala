package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-24 12:16
 * @Description 用户性别标签模型
 */
object GenderModel extends Logging {
  def main(args: Array[String]): Unit = {

    //todo： 1 创建SparkSession实例对象
    val spark = {
      // 1.a 创建SparkConf 设置应用信息
      val sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.sql.shuffle.partitions", "4")
        //从HBase表读取数据  设置序列化
        .set("spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable],
            classOf[Result], classOf[Put])
        )
      //1.b 建造者模式构建SparkSession对象
      val session = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
      //1.c 返回回话实例对象
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
      |WHERE id = 318
      |UNION
      |SELECT `id`,
      |       `name`,
      |       `rule`,
      |       `level`
      |FROM `profile_tags`.`tbl_basic_tag`
      |WHERE pid = 318
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
    //测试打印数据
    //basicTagDF.printSchema()
    //basicTagDF.show(10, false)

    /**
     * root
     * |-- id: long (nullable = false)
     * |-- name: string (nullable = true)
     * |-- rule: string (nullable = true)
     * |-- level: integer (nullable = true)
     */
    //标签数据会多次使用 进行缓存
    basicTagDF.persist(StorageLevel.MEMORY_ONLY_2).count()
    // TODO: 3. 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
    //3.1 获取业务标签数据的标签规则 rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4) //四级标签里有rule的介绍
      .head() //获取第一条数据
      .getAs[String]("rule")
    //logWarning(s"==========${tagRule}==========")
    /*
      inType=hbase
      zkHosts=bigdata-cdh01.itcast.cn
      zkPort=2181
      hbaseTable=tbl_tag_users
      family=detail
      selectFieldNames=id,gender
     */
    //3.2 解析规则 存储至Map集合
    val ruleMap: Map[String, String] = tagRule
      //按照换行符 对数据进行分割
      .split("\\n")
      .map { line =>
        val Array(attrKey, attrValue) = line.trim.split("=")
        attrKey -> attrValue
      }
      //由于数组是二元组 可以直接转为Map集合
      .toMap
    //logWarning(s"=============${ruleMap}=============")

    /**
     * Map(inType -> hbase, zkHosts -> bigdata-cdh01.itcast.cn,
     * zkPort -> 2181, hbaseTable -> tbl_tag_users,
     * selectFieldNames -> id,gender, family -> detail)
     */

    //3.3 判断数据源 依据数据源获取业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase)) {
      //将Map集合中数据源信息封装到Meta表中
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      //从HBase表加载业务数据
      businessDF = HBaseTools.read(
        spark, hbaseMeta.zkHosts, hbaseMeta.zkPort,
        hbaseMeta.hbaseTable, hbaseMeta.family,
        hbaseMeta.selectFieldNames.split(",")
      )
    } else {
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法进行标签计算")
    }
    //businessDF.printSchema()
    /**
     * root
     * |-- id: string (nullable = true)
     * |-- gender: string (nullable = true)
     */
    // TODO: 4. 业务数据和属性标签结合，构建标签：规则匹配型标签 -> rule match
    //4.1 首先获取属性标签数据：id -> tagId ,rule
    val attrTagDF: DataFrame = basicTagDF
      .filter($"level" === 5)
      .select(
        $"id".as("tagId"),
        $"rule"
      )
    //attrTagDF.show(10,false)
    /**
     * +-----+----+
     * |tagId|rule|
     * +-----+----+
     * |319  |1   |
     * |320  |2   |
     * +-----+----+
     */
    //4.2 将业务数据和属性标签进行关联Join  业务字段 = rule
    val joinDF: DataFrame = businessDF.join(
      attrTagDF, businessDF("gender") === attrTagDF("rule")
    )
    //joinDF.show()
    /**
     * +---+------+-----+----+
     * | id|gender|tagId|rule|
     * +---+------+-----+----+
     * |  1|     2|  320|   2|
     * | 10|     2|  320|   2|
     * |100|     2|  320|   2|
     * |101|     1|  319|   1|
     * |102|     2|  320|   2|
     * |103|     1|  319|   1|
     * |104|     1|  319|   1|
     * +---+------+-----+----+
     */
    //4.3 提取字段 uid 和 tagId
    val modelDF: DataFrame = joinDF.select(
      $"id".as("uid"), $"tagId".cast(StringType)
    )
    //modelDF.show()
    // 当缓存数据不再被使用时，释放资源
    basicTagDF.unpersist()
    // TODO: 5. 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile
    //5.1 获取HBase数据库中历史画像标签表的数据：ProfileDF -> tbl_profile
    val profileDF: DataFrame = HBaseTools.read(
      spark, "bigdata-cdh01.itcast.cn", "2181",
      "tbl_profile", "user", Seq("userId", "tagIds")
    )
    //5.2 合并当前标签数据和历史画像标签数据： newProfileDF
    //a. 按照用户ID关联数据 使用左外连接
    val mergeDF: DataFrame = modelDF.join(
      profileDF, modelDF("uid") === profileDF("userId"), "left"
    )

    /**
     * root
     * |-- uid: string (nullable = true)
     * |-- tagId: string (nullable = false)
     * |-- userId: string (nullable = true)
     * |-- tagIds: string (nullable = true)
     */
    //mergeDF.printSchema()
    //mergeDF.show(10,false)
    //b. 自定义UDF函数  合并标签
    val merge_tags_udf: UserDefinedFunction = udf(
      (tagId: String, tagIds: String) => {
        tagIds.split(",").:+(tagId).distinct.mkString(",")
      }
    )
    //c. 获取最新用户画像标签数据
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"),
      when($"tagIds".isNull, $"tagId")
        .otherwise(merge_tags_udf($"tagId", $"tagIds"))
        .as("tagIds")
    )

    /**
     * root
     * |-- userId: string (nullable = true)
     * |-- tagIds: string (nullable = true)
     */
    //newProfileDF.printSchema()
    //newProfileDF.show(10,false)
    //5.3 保存最新的画像标签数据至HBase数据库
    HBaseTools.write(
      newProfileDF, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId"
    )
    // 应用结束，关闭资源
    spark.stop()
  }
}
