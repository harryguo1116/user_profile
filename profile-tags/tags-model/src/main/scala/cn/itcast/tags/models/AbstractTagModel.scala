package cn.itcast.tags.models

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.meta.MetaParse
import cn.itcast.tags.tools.ProfileTools
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-25 21:22
 * @Description 所有标签模型的基类，必须继承此类，实现doTag方法，如何计算标签
 */
abstract class AbstractTagModel(modelName: String, modelType: ModelType) extends Logging {

  // 变量声明
  var spark: SparkSession = _

  // 1. 初始化：构建SparkSession实例对象
  def init(isHive: Boolean = false): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass, isHive)
  }

  // 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
  def getTagData(tagId: Long): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("dbtable", ModelConfig.tagTable(tagId))
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
  }

  // 3. 业务数据：依据业务标签规则rule，从数据源获取业务数据
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    // a. 获取业务标签规则rule，解析为Map集合
    val paramsMap: Map[String, String] = MetaParse.parseRuleToParams(tagDF)
    // b. 加载业务数据
    val businessDF: DataFrame = MetaParse.parseMetaToData(spark, paramsMap)
    // c. 返回业务数据
    businessDF
  }

  // 4. 构建标签：依据业务数据和属性标签数据建立标签
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  // 5. 标签合并与保存：读取用户标签数据，进行合并操作，最后保存
  def mergeAndSaveTag(modelDF: DataFrame): Unit = {
    // 5.a. 从HBase表中读取户画像标签表数据: userId、tagIds
    val profileDF: DataFrame = ProfileTools.loadProfile(spark)
    // 5.b. 将用户标签数据合并
    val newProfileDF = ProfileTools.mergeProfileTags(modelDF, profileDF)
    // 5.c. 保存HBase表中
    ProfileTools.saveProfile(newProfileDF)
  }

  // 6. 关闭资源：应用结束，关闭会话实例对象
  def close(): Unit = {
    if (null != spark) spark.close()
  }

  // 规定标签模型执行流程顺序
  def executeModel(tagId: Long, isHive: Boolean = false): Unit = {
    // a. 初始化
    init(isHive)
    try {
      // b. 获取标签数据
      val tagDF: DataFrame = getTagData(tagId)
      //      println("========tagDF=========")
      //      tagDF.show()
      tagDF.persist(StorageLevel.MEMORY_ONLY_2).count()

      // c. 获取业务数据
      val businessDF: DataFrame = getBusinessData(tagDF)
      //      println("========businessDF=========")
      //      businessDF.show()

      // d. 计算标签
      val modelDF: DataFrame = doTag(businessDF, tagDF)
      //      println("========modelDF=========")
      //      modelDF.show()

      // e. 合并标签与保存
      //      mergeAndSaveTag(modelDF)
      //      println("save success")

      tagDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close()
    }
  }

}
