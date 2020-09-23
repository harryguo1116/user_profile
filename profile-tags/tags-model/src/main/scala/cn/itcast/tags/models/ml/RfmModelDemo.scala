package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-28 18:02
 * @Description 挖掘类型标签模型开发：用户客户价值标签模型（RFM模型）
 */
class RfmModelDemo extends AbstractModel("用户客户价值标签", ModelType.ML) {

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
    import tagDF.sparkSession.implicits._
    //    businessDF.printSchema()
    /*
    root
       |-- memberid: string (nullable = true)
       |-- ordersn: string (nullable = true)
       |-- orderamount: string (nullable = true)
       |-- finishtime: string (nullable = true)
     */

    // todo 1- 从订单数据获取字段值，计算每个用户RFM值
    val rfmDF: DataFrame = businessDF
      //按照用户进行分组
      .groupBy($"memberid")
      //计算rfm的值
      .agg(
        max($"finishtime").as("finish_time"),
        count($"ordersn").as("frequency"),
        sum($"orderamount".cast(DataTypes.createDecimalType(10, 2))).as("monetary")
      )
      .select(
        $"memberid".as("uid"),
        datediff(current_timestamp(), from_unixtime($"finish_time")).as("recency"),
        $"frequency", $"monetary"
      )
    //    rfmDF.printSchema()
    //    rfmDF.show()
    /*
    root
     |-- uid: string (nullable = true)
     |-- recency: integer (nullable = true)
     |-- frequency: long (nullable = false)
     |-- monetary: decimal(20,2) (nullable = true)

    +---+-------+---------+---------+
    |uid|recency|frequency| monetary|
    +---+-------+---------+---------+
    |  1|     78|      240|528430.41|
    |102|     78|      107|141953.90|
    +---+-------+---------+---------+
     */

    // todo： 2- 按照规则，给RFM值打分Score
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
      .when(col("monetary").between(10000, 49999), 2.0) //
      .when(col("monetary").between(50000, 99999), 3.0) //
      .when(col("monetary").between(100000, 199999), 4.0) //
      .when(col("monetary").geq(200000), 5.0) //
    // 基于规则对RFM值进行打分
    val rfmScoreDF: DataFrame = rfmDF
      .select(
        $"uid",
        rWhen.as("r_score"),
        fWhen.as("f_score"),
        mWhen.as("m_score")
      )
    //    rfmScoreDF.printSchema()
    //    rfmScoreDF.show()
    /*
    root
       |-- uid: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)

      +---+-------+-------+-------+
      |uid|r_score|f_score|m_score|
      +---+-------+-------+-------+
      |  1|    1.0|    5.0|    5.0|
      |102|    1.0|    3.0|    4.0|
      |107|    1.0|    3.0|    5.0|
      |110|    1.0|    3.0|    5.0|
      +---+-------+-------+-------+
     */

    // todo 3- 将RFM数据使用KMeans算法聚类（K=5个）
    // 3.1 组合r、f、m到特征向量vector中
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("raw_features")
    val raw_featuresDF: DataFrame = assembler.transform(rfmScoreDF)
    //    raw_featuresDF.printSchema()
    //    raw_featuresDF.show()
    /*
    root
       |-- uid: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)
       |-- raw_features: vector (nullable = true)

      +---+-------+-------+-------+-------------+
      |uid|r_score|f_score|m_score| raw_features|
      +---+-------+-------+-------+-------------+
      |  1|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|
      |102|    1.0|    3.0|    4.0|[1.0,3.0,4.0]|
      |107|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|
      |110|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|
      +---+-------+-------+-------+-------------+
     */
    //缓存特征向量DF
    //todo 模型调优方式一  对数据进行归一化处理 数据处理为【0-1】的数值
    val minMaxScaler: MinMaxScalerModel = new MinMaxScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMin(0).setMax(1)
      .fit(raw_featuresDF)
    val featuresDF: DataFrame = minMaxScaler.transform(raw_featuresDF)
    //    featuresDF.printSchema()
    //    featuresDF.show()
    /*
    root
       |-- uid: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)
       |-- raw_features: vector (nullable = true)
       |-- features: vector (nullable = true)

      +---+-------+-------+-------+-------------+--------------------+
      |uid|r_score|f_score|m_score| raw_features|            features|
      +---+-------+-------+-------+-------------+--------------------+
      |  1|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|       [0.5,1.0,1.0]|
      |102|    1.0|    3.0|    4.0|[1.0,3.0,4.0]|[0.5,0.3333333333...|
      |107|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|[0.5,0.3333333333...|
      |110|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|[0.5,0.3333333333...|
      +---+-------+-------+-------+-------------+--------------------+
     */
    // 3.2 使用KMeans算法，训练模型
    // val kMeansModel: KMeansModel = trainModel(featuresDF)
    // wssse = 0.11087164750983236
    // 调整超参数获取最佳模型
    //val kMeansModel: KMeansModel =trainBestModel(featuresDF)
    // 加载模型，如果存在就加载；不存在，训练获取，并保存
    val kMeansModel: KMeansModel = loadModel(featuresDF)

    //3.3 获取类簇中心点
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    //把列簇和索引进行拉链合并操作
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
      .zipWithIndex
      // 获取类簇中心点向量之和（RFM之和）
      .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
      .sortBy { case (rfm, _) => -rfm }
      .zipWithIndex
    //    clusterIndexArray.foreach(println)
    /*
      ((2.5,3),0)
      ((1.834051724137931,0),1)
      ((1.5,4),2)
      ((0.833333333333333,2),3)
      ((0.5,1),4)
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
         |-- raw_features: vector (nullable = true)
         |-- features: vector (nullable = true)
         |-- prediction: integer (nullable = true)

        +---+-------+-------+-------+-------------+--------------------+----------+
        |uid|r_score|f_score|m_score| raw_features|            features|prediction|
        +---+-------+-------+-------+-------------+--------------------+----------+
        |  1|    1.0|    5.0|    5.0|[1.0,5.0,5.0]|       [0.5,1.0,1.0]|         3|
        |102|    1.0|    3.0|    4.0|[1.0,3.0,4.0]|[0.5,0.3333333333...|         2|
        |107|    1.0|    3.0|    5.0|[1.0,3.0,5.0]|[0.5,0.3333333333...|         0|
        +---+-------+-------+-------+-------------+--------------------+----------+
     */
    // 4）、从KMeans中获取出每个用户属于簇
    // 4.1 获取属性标签数据：rule, tagId
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
    // 4.2 对类簇中心点数据进行遍历，获取对应tagId
    val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((rfmScore, clusterIndex), index) =>
      val tagId: Long = ruleMap(index.toString)
      //返回列簇中心点和tagId
      (clusterIndex, tagId)
    }.toMap

    //    clusterTagMap.foreach(println)
    /*
    (0,363)
    (1,366)
    (2,365)
    (3,362)
    (4,364)
     */

    // 4.3 自定义UDF函数，传递prediction，返回tagId
    val index_to_TagId: UserDefinedFunction = udf(
      (clusterIndex: Int) => clusterTagMap(clusterIndex)
    )
    // 4.4 对预测数据，打标签
    val modelDF: DataFrame = predictionDF
      .select(
        $"uid",
        index_to_TagId($"prediction").as("tagId")
      )
    modelDF
    /*
      +---+-----+
      |uid|tagId|
      +---+-----+
      |  1|  362|
      |102|  365|
      |107|  363|
      |110|  363|
      +---+-----+
     */
  }

  //提取方法
  def trainModel(dataFrame: DataFrame): KMeansModel = {
    val kMeans: KMeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(5)
      .setMaxIter(20)
    val kMeansModel: KMeansModel = kMeans.fit(dataFrame)
    //误差平方和
    val wssse: Double = kMeansModel.computeCost(dataFrame)
    //    println(wssse)
    //返回模型
    kMeansModel
  }

  /**
   * 调整算法超参数，获取最佳模型
   *
   * @param dataFrame 数据集
   * @return
   */
  def trainBestModel(dataFrame: DataFrame): KMeansModel = {
    // TODO：模型调优方式二：调整算法超参数 -> MaxIter 最大迭代次数, 使用训练验证模式完成
    // 1. 设置MaxIter最大迭代次数：5， 10， 20
    val maxIter = Array(5, 10, 20)
    val models: Array[(Array[Int], Double, KMeansModel)] = maxIter.map { iter =>
      val kMeans: KMeans = new KMeans()
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setK(5)
        .setMaxIter(iter)
      val kMeansModel: KMeansModel = kMeans.fit(dataFrame)
      //误差平方和
      val wssse: Double = kMeansModel.computeCost(dataFrame)
      println(s"${iter} -> ${wssse}")
      // iv. 返回三元组：maxIter, wssse, model
      (maxIter, wssse, kMeansModel)
    }
    val (_, _, bestModel) = models.minBy(tuple => tuple._2)
    bestModel
  }

  /**
   * 加载模型，如果模型不存在，使用算法训练模型
   *
   * @param dataFrame 训练数据集
   * @return KMeansModel 模型
   */
  def loadModel(dataFrame: DataFrame): KMeansModel = {
    // 1. 模型保存路径
    val modelPath:String = ModelConfig.MODEL_BASE_PATH + s"${this.getClass.getSimpleName}"
    // 获取Hadoop配置信息
    val conf: Configuration = dataFrame.sparkSession.sparkContext.hadoopConfiguration
    // 2. 判断路径是否存在
    val kmeansModel: KMeansModel = if (HdfsUtils.exists(conf, modelPath)) {
      //存在
      logWarning("文件存在可以直接加载===")
      KMeansModel.load(modelPath)
    } else {
      //不存在
      logWarning("文件不存在，正在训练最佳模型===")
      val bestModel: KMeansModel = trainBestModel(dataFrame)
      logWarning("最佳模型已经训练完毕 正在保存===")
      bestModel.save(modelPath)
      bestModel
    }
    kmeansModel
  }
}

object RfmModelDemo {
  def main(args: Array[String]): Unit = {

    val model = new RfmModelDemo()
    model.executeModel(361L)
  }
}





