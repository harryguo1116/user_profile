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
class RfmTagModel extends AbstractModel("用户客户价值标签", ModelType.ML) {

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

    // todo 1- 从订单数据获取字段值，计算每个用户RFM值
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
    //rfmDF.show(false)


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

    // todo 3- 将RFM数据使用KMeans算法聚类（K=5个）
    // 3.1 组合r、f、m到特征向量vector中
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("raw_features")
    val rfmFeaturesDF: DataFrame = assembler.transform(rfmScoreDF)
    //    rfmFeaturesDF.printSchema()
    //    rfmFeaturesDF.show(10, false)
    //缓存特征向量DF

    //todo 模型调优方式一  对数据进行归一化处理 数据处理为【0-1】的数值
    val minMaxScalerModel: MinMaxScalerModel = new MinMaxScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMin(0.0).setMax(1.0)
      .fit(rfmFeaturesDF)
    val featuresDF: DataFrame = minMaxScalerModel.transform(rfmFeaturesDF)
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    //    featuresDF.show(false)
    /*
    +-------+-------+-------+-------------+----------------------------+
    |r_score|f_score|m_score|raw_features |features                    |
    +-------+-------+-------+-------------+----------------------------+
    |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|[0.5,1.0,1.0]               |
    |1.0    |3.0    |4.0    |[1.0,3.0,4.0]|[0.5,0.3333333333333333,0.0]|
    |1.0    |3.0    |5.0    |[1.0,3.0,5.0]|[0.5,0.3333333333333333,1.0]|
    |1.0    |3.0    |5.0    |[1.0,3.0,5.0]|[0.5,0.3333333333333333,1.0]|
    |1.0    |2.0    |5.0    |[1.0,2.0,5.0]|[0.5,0.0,1.0]               |
    |1.0    |3.0    |5.0    |[1.0,3.0,5.0]|[0.5,0.3333333333333333,1.0]|
    +-------+-------+-------+-------------+----------------------------+
     */


    // 3.2 使用KMeans算法，训练模型
    // val kMeansModel: KMeansModel = trainModel(featuresDF)
    // 调整超参数获取最佳模型
    //val kMeansModel: KMeansModel = trainBestModel(featuresDF)
    // 加载模型，如果存在就加载；不存在，训练获取，并保存
    val kMeansModel: KMeansModel = loadModel(featuresDF)


    //3.3 获取类簇中心点
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    //clusterCenters.foreach(println)

    //把列簇和索引进行拉链合并操作
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
      .zipWithIndex
      // 获取类簇中心点向量之和（RFM之和）
      .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
      .sortBy { case (rfm, _) => -rfm }
      .zipWithIndex
    //    clusterIndexArray.foreach(println)

    // 3.4 使用模型对数据进行预测：划分类簇，属于哪个类别
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    //    predictionDF.printSchema()
    //    predictionDF.show()

    featuresDF.unpersist()
    // 4）、从KMeans中获取出每个用户属于簇
    // 4.1 获取属性标签数据：rule, tagId
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
    //    ruleMap.foreach(println)

    // 4.2 对类簇中心点数据进行遍历，获取对应tagId
    val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((rfm_score, clusterIndex), index) =>
      val tagId: Long = ruleMap(index.toString)
      //返回列簇中心点和tagId
      (clusterIndex, tagId)
    }.toMap
    //   clusterTagMap.foreach(println)

    // 4.3 自定义UDF函数，传递prediction，返回tagId
    val index_to_tag: UserDefinedFunction = udf(
      (clusterIndex: Int) => clusterTagMap(clusterIndex)
    )
    // 4.4 对预测数据，打标签
    val modelDF: DataFrame = predictionDF.select(
      $"uid", //
      index_to_tag($"prediction").as("tagId")
    )

    modelDF

  }

  //提取方法
  def trainModel(dataFrame: DataFrame): KMeansModel = {
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(10)
      .setK(5)
      .setInitMode("k-means||")
    kmeans
    val kMeansModel: KMeansModel = kmeans.fit(dataFrame)
    //误差平方和
    val wssse: Double = kMeansModel.computeCost(dataFrame)
    println(wssse) //0.9978448275827247
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
    val maxIters: Array[Int] = Array(5, 10, 20)
    val models: Array[(Double, Int, KMeansModel)] = maxIters.map { iter =>
      val kmeans = new KMeans()
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setMaxIter(iter)
        .setK(5)
        .setInitMode("k-means||")
      val kmeansModel: KMeansModel = kmeans.fit(dataFrame)
      //求误差平方和
      val wssse = kmeansModel.computeCost(dataFrame)
      println(wssse)
      // iv. 返回三元组：maxIter, wssse, model
      (wssse, iter, kmeansModel)
    }
    val (_, _, bestModel) = models.minBy(tuple => tuple._1)
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
    val modelPath: String = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"
    // 获取Hadoop配置信息
    val conf: Configuration = dataFrame.sparkSession.sparkContext.hadoopConfiguration
    // 2. 判断路径是否存在
    val model: KMeansModel = if (HdfsUtils.exists(conf, modelPath)) {
      logWarning(s"文件有最佳模型，正在从【$modelPath】加载")
      KMeansModel.load(modelPath)
    } else {
      logWarning("没有最佳模型，正在训练最佳模型. . .")
      val bestModel: KMeansModel = trainBestModel(dataFrame)
      logWarning(s"最佳模型已训练好，正在保存到文件【$modelPath】中")
      bestModel.save(modelPath)
      bestModel
    }
    model
  }
}

object RfmTagModel {
  def main(args: Array[String]): Unit = {

    val model = new RfmTagModel()
    model.executeModel(361L)
  }
}



