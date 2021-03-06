package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-28 18:02
 * @Description  挖掘类型标签模型开发：用户客户价值标签模型（RFM模型）
 */
class RfmModel extends AbstractModel("用户客户价值标签", ModelType.ML) {
  /*
    361,客户价值
      362,高价值,0
      363,中上价值,1
      364,中价值,2
      365,中下价值,3
      366,超低价值,4
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //导入隐式转换
    import businessDF.sparkSession.implicits._
    //打印业务数据 获取schema信息
    /*
      root
        |-- memberid: string (nullable = true)
        |-- ordersn: string (nullable = true)
        |-- orderamount: string (nullable = true)
        |-- finishtime: string (nullable = true)
    */
    //businessDF.printSchema()
    /*
    +--------+----------------------+-----------+----------+
    |memberid|ordersn               |orderamount|finishtime|
    +--------+----------------------+-----------+----------+
    |232     |gome_792756751164275  |2479.45    |1590249600|
    |190     |jd_14090106121770839  |2449.00    |1591545600|
    +--------+----------------------+-----------+----------+
     */
    //businessDF.show(10,false)


    // 1）、从订单数据获取字段值，计算每个用户RFM值
    val rfmDF: DataFrame = businessDF
      .groupBy($"memberid")
      .agg(
        // R值 最近一次的消费时间  获取max(finishtime)的最大值
        max($"finishtime").as("finish_time"),
        // F值 最近一段时间内购买的次数 count(ordersn)
        count($"ordersn").as("frequency"),
        // M值 最近一段时间购买的总金额 sum(orderamount)
        sum($"orderamount".cast(DataTypes.createDecimalType())).as("monetary")
      )
      .select(
        $"memberid".as("uid"),
        datediff(current_date(), from_unixtime($"finish_time")).as("recency"),
        $"frequency",
        $"monetary"
      )
    //rfmDF.printSchema()
    /*
    root
    |-- uid: string (nullable = true)
    |-- recency: integer (nullable = true)
    |-- frequency: long (nullable = false)
    |-- monetary: decimal(20,0) (nullable = true)
    */
    //rfmDF.show(false)
    /*
      +---+-------+---------+--------+
      |uid|recency|frequency|monetary|
      +---+-------+---------+--------+
      |1  |77     |240      |528430  |
      |102|77     |107      |141954  |
      +---+-------+---------+--------+
     */

    // 2）、按照规则，给RFM值打分Score
    /*
      R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
      F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
      M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
     */
    // R 打分判断条件
    val rWhen: Column = when($"recency".between(1, 3), 5.0)
      .when($"recency".between(4, 6), 4.0)
      .when($"recency".between(7, 9), 3.0)
      .when($"recency".between(10, 15), 2.0)
      .when($"recency".gt(16), 1.0)
    // F 打分条件表达式
    val fWhen = when(col("frequency").between(1, 49), 1.0) //
      .when(col("frequency").between(50, 99), 2.0) //
      .when(col("frequency").between(100, 149), 3.0) //
      .when(col("frequency").between(150, 199), 4.0) //
      .when(col("frequency").geq(200), 5.0) //
    // M 打分条件表达式
    val mWhen = when(col("monetary").lt(10000), 1.0) //
      .when($"monetary".between(10000, 49999), 2.0) //
      .when($"monetary".between(50000, 99999), 3.0) //
      .when($"monetary".between(100000, 199999), 4.0) //
      .when($"monetary".geq(200000), 5.0) //
    // 基于规则对RFM值进行打分
    val rfmScoreDF: DataFrame = rfmDF
      .select(
        $"uid", rWhen.as("r_score"), //
        fWhen.as("f_score"), mWhen.as("m_score")
      )
    //    rfmScoreDF.printSchema()
    //    rfmScoreDF.show(false)
    /*
    root
    |-- uid: string (nullable = true)
    |-- r_score: double (nullable = true)
    |-- f_score: double (nullable = true)
    |-- m_score: double (nullable = true)

    +---+-------+-------+-------+
    |uid|r_score|f_score|m_score|
    +---+-------+-------+-------+
    |1  |1.0    |5.0    |5.0    |
    |102|1.0    |3.0    |4.0    |
    |107|1.0    |3.0    |5.0    |
    |110|1.0    |3.0    |5.0    |
    +---+-------+-------+-------+
    */

    // 3）、将RFM数据使用KMeans算法聚类（K=5个）
    // 3.1 组合r、f、m到特征向量vector中
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(rfmScoreDF)
    //featuresDF.printSchema()
    //featuresDF.show(10, false)
    //featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    /*
    root
       |-- uid: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)
       |-- features: vector (nullable = true)

      +---+-------+-------+-------+-------------+
      |uid|r_score|f_score|m_score|features     |
      +---+-------+-------+-------+-------------+
      |1  |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|
      |102|1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
      |107|1.0    |3.0    |5.0    |[1.0,3.0,5.0]|
      |110|1.0    |3.0    |5.0    |[1.0,3.0,5.0]|
      +---+-------+-------+-------+-------------+
     */
    // 3.2 使用KMeans算法，训练模型
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(10)
      .setK(5)
      .setInitMode("k-means||")
    val kMeansModel: KMeansModel = kmeans.fit(featuresDF)

    //误差平方和
    val wssse: Double = kMeansModel.computeCost(featuresDF)
    //println(wssse) //0.9978448275827247
    //3.3 获取类簇中心点
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    //clusterCenters.foreach(println)
    /*
    +---+-------+-------+-------+-------------+----------+
    |uid|r_score|f_score|m_score|     features|prediction|
    +---+-------+-------+-------+-------------+----------+
    |  1|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|         2|
    |102|    1.0|    3.0|    4.0|[1.0,3.0,4.0]|         0|
    |107|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|         1|
    |110|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|         1|
    +---+-------+-------+-------+-------------+----------+
     */
    //把列簇和索引进行拉链合并操作
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
      .zipWithIndex
      // 获取类簇中心点向量之和（RFM之和）
      .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
      .sortBy { case (rfm, _) => -rfm }
      .zipWithIndex
    //    clusterIndexArray.foreach(println)
    /*
    ((11.0,2),0)
    ((9.002155172413794,1),1)
    ((8.0,0),2)
    ((8.0,4),3)
    ((7.0,3),4)
     */
    // 3.4 使用模型对数据进行预测：划分类簇，属于哪个类别
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    //    predictionDF.printSchema()
    //    predictionDF.show()
    /*
      root
         |-- uid: string (nullable = true)
         |-- r_score: double (nullable = true)
         |-- f_score: double (nullable = true)
         |-- m_score: double (nullable = true)
         |-- features: vector (nullable = true)
         |-- prediction: integer (nullable = true)

      +---+-------+-------+-------+-------------+----------+
      |uid|r_score|f_score|m_score|     features|prediction|
      +---+-------+-------+-------+-------------+----------+
      |  1|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|         2|
      |102|    1.0|    3.0|    4.0|[1.0,3.0,4.0]|         0|
      |107|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|         1|
      |110|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|         1|
      |111|    1.0|    2.0|    5.0|[1.0,2.0,5.0]|         4|
      |120|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|         1|
      +---+-------+-------+-------+-------------+----------+

     */

    // 4）、从KMeans中获取出每个用户属于簇
    // 4.1 获取属性标签数据：rule, tagId
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
//    ruleMap.foreach(println)
    /*
    (4,366)
    (1,363)
    (0,362)
    (2,364)
    (3,365)
     */
    // 4.2 对类簇中心点数据进行遍历，获取对应tagId
    val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((rfm_score, clusterIndex), index) =>
      val tagId: Long = ruleMap(index.toString)
      //返回列簇中心点和tagId
      (clusterIndex, tagId)
    }.toMap
    //   clusterTagMap.foreach(println)
    /*
    (4,366)
    (1,363)
    (0,362)
    (2,364)
    (3,365)
    (0,364)
     */
    // 4.3 自定义UDF函数，传递prediction，返回tagId
    val index_to_tag: UserDefinedFunction = udf(
      (clusterIndex: Int) => clusterTagMap(clusterIndex)
    )
    // 4.4 对预测数据，打标签
    val modelDF: DataFrame = predictionDF.select(
      $"uid", //
      index_to_tag($"prediction").as("tagId")
    )
    //    modelDF.printSchema()
    //    modelDF.show()
    /*
    root
       |-- uid: string (nullable = true)
       |-- tagId: long (nullable = true)
    +---+-----+
    |uid|tagId|
    +---+-----+
    |  1|  362|
    |102|  364|
    |107|  363|
    |110|  363|
    |111|  365|
    |120|  363|
    +---+-----+
     */
    modelDF

  }
}


object RfmModel {
  def main(args: Array[String]): Unit = {

    val model = new RfmModel()
    model.executeModel(361L)
  }
}
