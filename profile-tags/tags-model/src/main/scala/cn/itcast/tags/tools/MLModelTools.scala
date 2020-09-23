package cn.itcast.tags.tools

import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-30 21:09
 * @Description
 */
object MLModelTools extends Logging {


  /**
   * 加载模型，如果模型不存在，使用算法训练模型
   *
   * @param dataframe 训练数据集
   * @param mlType    表示标签模型名称，对应使用具体算法训练模型
   * @param modelPath 算法模型保存路径
   * @return Model 算法模型
   */
  def loadModel(dataframe: DataFrame,
                mlType: String,
                modelPath: String): Model[_] = {

    val conf = dataframe.sparkSession.sparkContext.hadoopConfiguration
    // 对保存的路径进行判断  有的话直接加载，没有的话先训练最佳模型 再保存
    if (HdfsUtils.exists(conf, modelPath)) {
      logWarning(s"正在从【$modelPath】加载模型.................")
      mlType.toLowerCase match {
        case "rfm" => KMeansModel.load(modelPath)
        case "rfe" => KMeansModel.load(modelPath)
        case "psm" => KMeansModel.load(modelPath)
        case "usg" => PipelineModel.load(modelPath)
      }
    } else {
      // 2. 如果模型不存在训练模型，获取最佳模型及保存模型
      logWarning(s"正在训练模型.................")
      val bestModel = mlType.toLowerCase match {
        case "rfm" => trainBestKMeansModel(dataframe, k = 5)
        case "rfe" => trainBestKMeansModel(dataframe, k = 4)
        case "psm" => trainBestKMeansModel(dataframe, k = 5)
        case "usg" => trainBestPipelineModel(dataframe)
      }
      // 3. 保存模型
      logWarning("保存最佳模型.................")
      bestModel.save(modelPath)
      // 4. 返回最佳模型
      bestModel
    }
  }


  /**
   * 调整算法超参数，获取最佳模型
   *
   * @param dataframe 数据集
   * @return
   */
  def trainBestKMeansModel(dataframe: DataFrame, k: Int = 2): KMeansModel = {

    // 1. 设置超参数的值
    val maxIter: Array[Int] = Array(5, 15, 20)
    // 2.不同超参数的值，训练模型
    dataframe.persist(StorageLevel.MEMORY_AND_DISK).count()
    val models: Array[(Double, KMeansModel, Int)] = maxIter.map { iter =>
      // 2.1. 使用KMeans算法训练数据
      val means: KMeans = new KMeans()
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setK(k)
        .setMaxIter(iter)
        .setSeed(31)
      // 2.2. 训练模式
      val kMeansModel: KMeansModel = means.fit(dataframe)
      // 2.3. 获取误差平方和wssse
      val wssse: Double = kMeansModel.computeCost(dataframe)
      // 2.4. 返回三元组 wssse,kMeansModel,iter
      (wssse, kMeansModel, iter)
    }
    models.foreach(println)
    dataframe.unpersist()
    // 3. 获取最佳模型
    val (wssse, bestModel, iter) = models.minBy(tuple => tuple._1)
    // 4. 返回模型
    bestModel
  }

  /**
   * 采用K-Fold交叉验证方式，调整超参数获取最佳PipelineModel模型
   * @param dataframe 数据集
   * @return
   */
  def trainBestPipelineModel(dataframe: DataFrame): PipelineModel = {
    // a. 特征向量化
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("color", "product"))
      .setOutputCol("raw_features")

    // b. 类别特征进行索引
    val indexer: VectorIndexer= new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMaxCategories(30)

    // c. 构建决策树分类器
    val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    // d. 构建Pipeline管道流实例对象
    val pipeline: Pipeline = new Pipeline().setStages(
      Array(assembler, indexer, dtc)
    )

    // e. 构建参数网格，设置超参数的值
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(dtc.maxDepth, Array(5))
      .addGrid(dtc.impurity, Array("gini", "entropy"))
      .addGrid(dtc.maxBins, Array(32))
      .build()

    // f. 多分类评估器
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      // 指标名称，支持：f1、weightedPrecision、weightedRecall、accuracy
      .setMetricName("accuracy")

    // g. 构建交叉验证实例对象
    val crossValidator: CrossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // h. 训练模式
    val crossValidatorModel: CrossValidatorModel = crossValidator.fit(dataframe)

    // i. 获取最佳模型
    val pipelineModel: PipelineModel = crossValidatorModel.bestModel
      .asInstanceOf[PipelineModel]

    // j. 返回模型
    pipelineModel
  }
}