package cn.itcast.tags.meta

import cn.itcast.tags.tools.TagTools
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author Harry
 * @Date 2020-08-30 20:21
 * @Description
 */
object MetaParse extends Logging {
  /**
   * 依据标签数据，获取业务标签规则rule，解析转换为Map集合
   *
   * @param tagDF 标签数据
   * @return Map集合
   */
  def parseRuleToParams(tagDF: DataFrame): Map[String, String] = {
    import tagDF.sparkSession.implicits._

    // 1. 4级标签规则rule
    val tagRule: String = tagDF
      .filter($"level" === 4)
      .head().getAs[String]("rule")
    //logWarning(s"==== 业务标签数据规则: {$tagRule} ====")

    // 2. 解析标签规则，先按照换行\n符分割，再按照等号=分割
    val ruleMap: Map[String, String] = tagRule
      .split("\n")
      .map { line =>
        val Array(attrName, attrValue) = line.trim.split("=")
        (attrName, attrValue)
      }
      .toMap
    //3. 返回对象
    ruleMap
  }


  /**
   * 依据inType判断数据源，封装元数据Meta，加载业务数据
   *
   * @param spark     SparkSession实例对象
   * @param paramsMap 业务数据源参数集合
   * @return
   */
  def parseMetaToData(spark: SparkSession,
                      paramsMap: Map[String, String]): DataFrame = {
    //1. 获取数据源
    val inType: String = paramsMap("inType").toLowerCase
    //2. 判断数据源 封装进meta 加载业务数据
    var businessDF: DataFrame = null
    inType match {
      case "hbase" =>
        // 把数据封装进meta
        val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(paramsMap)
        // 依据条件到HBase中获取业务数据
        businessDF = spark.read
          .format("hbase")
          .option("zkHosts", hbaseMeta.zkHosts)
          .option("zkPort", hbaseMeta.zkPort)
          .option("hbaseTable", hbaseMeta.hbaseTable)
          .option("family", hbaseMeta.family)
          .option("selectFields", hbaseMeta.selectFieldNames)
          .option("filterConditions", hbaseMeta.filterConditions)
          .load()
      case "mysql" =>
        // 规则数据封装到MySQLMeta中
        val mysqlMeta = MySQLMeta.getMySQLMeta(paramsMap)
        // 依据条件加载MySQL表业务数据
        businessDF = spark.read
          .format("jdbc")
          .option("driver", mysqlMeta.driver)
          .option("url", mysqlMeta.url)
          .option("user", mysqlMeta.user)
          .option("password", mysqlMeta.password)
          .option("dbtable", mysqlMeta.sql)
          .load()
      case "hive" =>
        //规则数据封装到HiveMeta表中
        val hiveMeta: HiveMeta = HiveMeta.getHiveMeta(paramsMap)
        // 加载Hive表中数据
        businessDF = spark.read
          .table(hiveMeta.hiveTable)
          .select(hiveMeta.selectFieldNames: _ *)
      case "hdfs" =>
        //规则数据封装到HdfsMeta对象中
        val hdfsMeta: HdfsMeta = HdfsMeta.getHdfsMeta(paramsMap)
        // 加载HDFS文件数据
        businessDF = spark.read
          .option("sep", hdfsMeta.sperator)
          .option("header", "true")
          .csv(hdfsMeta.inPath)
          .select(hdfsMeta.selectFieldNames: _*)
      case "json" =>
        //规则数据封装到JsonMeta对象中
        val jsonMeta: JsonMeta = JsonMeta.getJsonMeta(paramsMap)
        // 加载HDFS文件数据
        businessDF = spark.read
          .option("sep", jsonMeta.sperator)
          .option("header", "true")
          .json(jsonMeta.inPath)
          .select(jsonMeta.selectFieldNames: _*)
      case "_" =>
        // 如果未获取到数据，直接抛出异常
        new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    // 3. 返回数据
    businessDF
  }

}
