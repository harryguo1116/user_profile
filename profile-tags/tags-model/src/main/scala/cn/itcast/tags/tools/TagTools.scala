package cn.itcast.tags.tools

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * @Author Harry
 * @Date 2020-08-25 14:22
 * @Description 针对标签进行相关操作工具类
 */
object TagTools {

  /**
   * 将[属性标签]数据中标签ID提取并转换为为Set集合
   *
   * @param tagDF 属性标签数据
   * @return Set 集合
   */
  def convertSet(tagDF: DataFrame): Set[String] = {
    import tagDF.sparkSession.implicits._
    tagDF
      // 获取属性标签数据
      .filter($"level" === 5)
      .rdd // 转换为RDD
      .map { row => row.getAs[Long]("id") }
      .collect()
      .map(_.toString)
      .toSet
  }

  /**
   * 将【属性标签】数据中规则 rule与标签ID tagId转为Map集合
   *
   * @param tagDF 属性标签数据
   * @return Map集合
   */
  def convertMap(tagDF: DataFrame): Map[String, Long] = {
    import tagDF.sparkSession.implicits._

    tagDF
      //获取属性标签数据
      .filter($"level" === 5)
      //选择标签规则rule和标签Id
      .select($"rule", $"id".as("tagId"))
      //转换为DataSet
      .as[(String, Long)]
      //转为RDD
      .rdd
      //转换为Map集合
      .collectAsMap().toMap
  }

  /**
   * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagId)
   *
   * @param dataframe 标签业务数据
   * @param field     标签业务字段
   * @param tagDF     标签数据
   * @return 标签模型数据
   */

  def ruleMatchTag(dataframe: DataFrame, field: String,
                   tagDF: DataFrame): DataFrame = {

    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._

    //1. 获取规则rule和tagId集合
    val ruleTagMap: Map[String, Long] = convertMap(tagDF)
    //2. 获取Map集合数据进行广播变量
    val ruleTagMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(ruleTagMap)
    //3. 自定义UDF函数，依据Job职业和属性标签规则进行标签化
    val field_to_tag = udf(
      (field: String) => ruleTagMapBroadcast.value(field)
    )
    //4. 计算标签 依据业务字段值获取标签ID
    val modelDF: DataFrame = dataframe
      .select(
        $"id".as("uid"),
        field_to_tag(col(field)).cast(StringType).as("tagId")
      )
    //modelDF.show()
    modelDF
  }

  /**
   * 将标签数据中属性标签规则rule拆分为范围: start, end
   *
   * @param tagDF 标签数据
   * @return 数据集DataFrame
   */
  def convertTuple(tagDF: DataFrame): DataFrame = {

    //导入隐式转化
    import tagDF.sparkSession.implicits._
    //1. 自定义UDF函数 把rule按照-进行分割成start-end
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("-").map(_.toInt)
        (start, end)
      })
    //2. 获取标签熟悉数据 解析TagID
    val attrTagDF: DataFrame = tagDF
      .filter($"level" === 5)
      .select(
        $"id".as("tagId"),
        rule_to_tuple($"rule").as("rules")
      )
      //获取开始和结束
      .select(
        $"tagId",
        $"rules._1".as("start"),
        $"rules._2".as("end")
      )
    attrTagDF
  }

  def convertIndexMap(clusterCenters: Array[linalg.Vector],
                      tagDF: DataFrame): Map[Int, Long] = {

    //1. 获取聚类模型中簇中心及索引
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
      .zipWithIndex
      // 获取类簇中心点向量之和
      .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
      .sortBy { case (rfm, _) => -rfm }
      .zipWithIndex
    //2. 获取标签属性的tagId
    val ruleMap: Map[String, Long] = convertMap(tagDF)
    //3. 组合uid和tagId
    val indexTagMap = clusterIndexArray.map { case ((vector, clusterIndex), index) =>
      val tagId: Long = ruleMap(index.toString)
      (clusterIndex, tagId)
    }.toMap
    indexTagMap
  }

  /**
   * KMeans 算法标签模型基于属性规则标签数据和KMeansModel预测值打标签封装方法
   * @param kmeansModel KMeans 算法模型
   * @param dataframe 预测数据集，含预测列prediction
   * @param tagDF 标签数据
   * @return 用户标签数据，含uid和tagId列
   */
  def kmeansMatchTag(kmeansModel: KMeansModel, dataframe: DataFrame,
                     tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 1 获取类簇中心节点
    val clusterCenters: Array[linalg.Vector] = kmeansModel.clusterCenters
    // 2 以类簇为中心 拉链操作索引
    val clusterTagMap: Map[Int, Long] = TagTools.convertIndexMap(clusterCenters, tagDF)
    //使用广播变量
    val clusterTagMapBroadcast: Broadcast[Map[Int, Long]] = spark.sparkContext.broadcast(clusterTagMap)
    //自定义udf函数  把clusterIndex转为tagId
    val index_to_tagId: UserDefinedFunction = udf(
      (clusterIndex: Int) => clusterTagMapBroadcast.value(clusterIndex)
    )
    //获取uid和tagId
    val modelDF: DataFrame = dataframe
      .select(
        $"uid",
        index_to_tagId($"prediction").as("tagId")
      )
    //3 返回数据
    modelDF
  }
}
