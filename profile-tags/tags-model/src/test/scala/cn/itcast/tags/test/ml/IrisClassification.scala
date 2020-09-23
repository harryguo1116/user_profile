package cn.itcast.tags.test.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * @Author Harry
 * @Date 2020-08-27 20:39
 * @Description 使用逻辑回归算法对鸢尾花数据集构建分类模型
 */
object IrisClassification {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    import spark.implicits._
    //自定义Schema信息
    val irisSchema = StructType(
      Array(
        StructField("sepal_length", DoubleType, nullable = true),
        StructField("sepal_width", DoubleType, nullable = true),
        StructField("petal_length", DoubleType, nullable = true),
        StructField("petal_width", DoubleType, nullable = true),
        StructField("category", StringType, nullable = true)
      )
    )
    //1、 读取原始数据集 鸢尾花数据 格式为csv格式
    val rawIrisDF = spark.read
      .schema(irisSchema)
      .option("sep", ",")
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("datas\\iris\\iris.data")
    //rawIrisDF.show()
    //2、 特征工程
    //2.1 类别特征转换
    val indexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")
      .fit(rawIrisDF)
    val df1 = indexerModel.transform(rawIrisDF)
    //df1.show(150,false)
    //2.2 组合特征值
    val assembler = new VectorAssembler()
      .setInputCols(rawIrisDF.columns.dropRight(1))
      .setOutputCol("raw_features")
    val df2 = assembler.transform(df1)
    //df2.show()
    //2.3 特征正则化
    val normalizer = new Normalizer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setP(2.0)
    val featuresDF = normalizer.transform(df2)
    //featuresDF.show()
    //todo 将特征数据划分为训练集和测试集
    val Array(trainDF,testDF) = featuresDF.randomSplit(Array(0.9,0.1),seed = 123L)
    // 由于使用逻辑回归分类算法，属于迭代算法，需要不停使用训练数据集训练模型，所以将训练缓存
    //trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count()
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()

    //3、 使用特征数据应用到算法中训练模型
    val Ir: LogisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      //设置迭代次数
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val IrModel = Ir.fit(featuresDF)
    //4、 使用模型预测
    val predictionDF = IrModel.transform(featuresDF)
    predictionDF
        .select("label","prediction")
        .show(150)
    //5、 模型评估 使用测试数据集应用到模型中 获取预测值
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    // ACCU = 0.9466666666666667
    println(s"ACCU = ${evaluator.evaluate(predictionDF)}")
    //6、 模型调优 暂时省略

    //7、 模型保存与加载
    // TODO: 保存模型
    val modelPath = s"datas/models/lrModel-${System.nanoTime()}"
    IrModel.save(modelPath)

    // TODO: 加载模型
    val loadLrModel = LogisticRegressionModel.load(modelPath)
    loadLrModel.transform(
      Seq(
        Vectors.dense(Array(5.1,3.5,1.4,0.2))
      )
        .map(x => Tuple1.apply(x))
        .toDF("features")
    ).show(1, truncate = false)
    //应用结束 关闭资源
    spark.stop()
  }
}
