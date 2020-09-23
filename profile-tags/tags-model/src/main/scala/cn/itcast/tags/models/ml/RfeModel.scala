package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * @Author Harry
 * @Date 2020-08-30 14:26
 * @Description 开发标签模型（挖掘类型标签）：用户活跃度RFE模型
 */

class RfeModel extends AbstractModel("用户活跃度RFE", ModelType.ML) {
  /*
    367 用户活跃度
       368,非常活跃,  0
       369,活跃,     1
       370,不活跃,   2
       371,非常不活跃,3

  */

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //导入隐式转换
    import tagDF.sparkSession.implicits._
    //businessDF.printSchema()
    /*
    root
       |-- global_user_id: string (nullable = true)
       |-- loc_url: string (nullable = true)
       |-- log_time: string (nullable = true)
     */

    //1. 从业务数据中获取需要的字段
    /**
     * Recency: 最近一次访问时间,用户最后一次访问距今时间
     * Frequency: 访问频率,用户一段时间内访问的页面总次数,
     * Engagements: 页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览量、下载量、 视频播放数量等
     */
    val rfeDF: DataFrame = businessDF
      .groupBy($"global_user_id")
      .agg(
        max($"log_time").as("last_time"),
        count($"loc_url").as("frequency"),
        countDistinct($"loc_url").as("engagements")
      )
      .select(
        $"global_user_id".as("uid"),
        datediff(
          date_sub(current_timestamp(), 300), $"last_time")
          .as("recency"), $"frequency", $"engagements"
      )
    //    rfeDF.printSchema()
    //    rfeDF.show(10, false)
    /*
    root
       |-- uid: string (nullable = true)
       |-- recency: integer (nullable = true)
       |-- frequency: long (nullable = false)
       |-- engagements: long (nullable = false)

        +---+-------+---------+-----------+
        |uid|recency|frequency|engagements|
        +---+-------+---------+-----------+
        |1  |14     |418      |270        |
        |102|14     |415      |271        |
        |139|14     |399      |255        |
        +---+-------+---------+-----------+
     */
    //2. 按照规则对数据进行打分
    //R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
    val rWhen: Column = when(col("recency").between(0, 15), 5.0)
      .when(col("recency").between(16, 30), 4.0)
      .when(col("recency").between(31, 45), 3.0)
      .when(col("recency").between(46, 60), 2.0)
      .when(col("recency").gt(61), 1.0)
    //F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
    val fWhen: Column = when(col("frequency").geq(400), 5.0)
      .when(col("frequency").between(300, 399), 4.0)
      .when(col("frequency").between(200, 299), 3.0)
      .when(col("frequency").between(100, 199), 2.0)
      .when(col("frequency").leq(99), 1.0)
    //E:≥250=5分，200-249=4分，150-199=3分，50-149=2分，≤49=1分
    val eWhen: Column = when(col("engagements").geq(250), 5.0)
      .when(col("engagements").between(200, 249), 4.0)
      .when(col("engagements").between(150, 199), 3.0)
      .when(col("engagements").between(50, 149), 2.0)
      .when(col("engagements").leq(49), 1.0)
    //3. 查询判断条件之后的数据
    val rfeScoreDF: DataFrame = rfeDF
      .select(
        $"uid",
        rWhen.as("r_score"),
        fWhen.as("f_score"),
        eWhen.as("e_score")
      )
    //rfeScoreDF.printSchema()
    //rfeScoreDF.show(10, false)
    /*
    root
     |-- uid: string (nullable = true)
     |-- r_score: double (nullable = true)
     |-- f_score: double (nullable = true)
     |-- e_score: double (nullable = true)

      +---+-------+-------+-------+
      |uid|r_score|f_score|e_score|
      +---+-------+-------+-------+
      |1  |5.0    |5.0    |5.0    |
      |102|5.0    |5.0    |5.0    |
      |107|5.0    |5.0    |5.0    |
      +---+-------+-------+-------+
     */

    // 4. 构建向量 把三个字段合并为一个
    val featuresDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "e_score"))
      .setOutputCol("features")
      .transform(rfeScoreDF)
    //featuresDF.printSchema()
    //featuresDF.show(10, truncate = false)
    /*
    root
     |-- uid: string (nullable = true)
     |-- r_score: double (nullable = true)
     |-- f_score: double (nullable = true)
     |-- e_score: double (nullable = true)
     |-- features: vector (nullable = true)

    +---+-------+-------+-------+-------------+
    |uid|r_score|f_score|e_score|features     |
    +---+-------+-------+-------+-------------+
    |1  |5.0    |5.0    |5.0    |[5.0,5.0,5.0]|
    |102|5.0    |5.0    |5.0    |[5.0,5.0,5.0]|
    |139|5.0    |4.0    |5.0    |[5.0,4.0,5.0]|
    +---+-------+-------+-------+-------------+
     */

    //5. 用kMeans计算模型
    val kMeansModel: KMeansModel = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(4)
      .setMaxIter(20)
      .fit(featuresDF)
    //误差平方和
    val wssse: Double = kMeansModel.computeCost(featuresDF)
    //println(wssse)

    //6. 对数值进行预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    //predictionDF.show()
    /*
    +---+-------+-------+-------+-------------+----------+
    |uid|r_score|f_score|e_score|     features|prediction|
    +---+-------+-------+-------+-------------+----------+
    |120|    1.0|    4.0|    5.0|[1.0,4.0,5.0]|         3|
    |130|    1.0|    5.0|    4.0|[1.0,5.0,4.0]|         2|
    |135|    1.0|    4.0|    4.0|[1.0,4.0,4.0]|         1|
    |137|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|         0|
    +---+-------+-------+-------+-------------+----------+
     */
    // 6.1 获取类簇中心节点
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    // 6.2 以类簇为中心 拉链操作索引
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
      .zipWithIndex
      .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
      .sortBy{case (rfe,_) => -rfe}
      .zipWithIndex
    // 7. 结合属性id给用户打标签
    // 7.1 获取属性标签数据：rule, tagId
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
        //对类簇中心点数据进行遍历，获取对应tagId
        val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((vector, clusterIndex), index) =>
          val tagId: Long = ruleMap(index.toString)
          (clusterIndex, tagId)
        }.toMap
    //使用广播变量
    val clusterTagMapBroadcast: Broadcast[Map[Int, Long]] = spark.sparkContext.broadcast(clusterTagMap)
    //自定义udf函数  把clusterIndex转为tagId
    val index_to_tagId: UserDefinedFunction = udf(
      (clusterIndex: Int) => clusterTagMapBroadcast.value(clusterIndex)
    )
    //获取uid和tagId
    val modelDF: DataFrame = predictionDF
      .select(
        $"uid",
        index_to_tagId($"prediction").as("tagId")
      )
//    modelDF.show(100,false)
    modelDF
  }
}

object RfeModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new RfeModel()
    tagModel.executeModel(367L)
  }
}