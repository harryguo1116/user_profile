package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.{MLModelTools, TagTools}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum, _}

/**
 * 挖掘类型标签模型开发：用户购物性别标签模型开发（USG 模型）
 */
class UsgTagModel extends AbstractModel("用户购物性别标签", ModelType.ML){
	/*
	378	用户购物性别
		379	男		0
		380	女		1
		381	中性		-1
	 */
	override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
		
		val spark: SparkSession = tagDF.sparkSession
		import spark.implicits._
		
		// 1. 获取订单表数据tbl_tag_orders，与订单商品表数据关联获取会员ID
		val ordersDF: DataFrame = spark.read
			.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_tag_orders")
			.option("family", "detail")
			.option("selectFields", "memberid,ordersn")
			.load()

		
		// 2. 加载维度表数据：tbl_dim_colors（颜色）、tbl_dim_products（产品）
		// 2.1 加载颜色维度表数据
		val colorsDF: DataFrame = {
			spark.read
				.format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url",
					"jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
						"useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
				.option("dbtable", "profile_tags.tbl_dim_colors")
				.option("user", "root")
				.option("password", "123456")
				.load()
		}

		// 2.2 构建颜色WHEN语句
		val colorColumn: Column = {
			// 声明可变变量
			var colorCol: Column = null
			// 循环遍历
			colorsDF.as[(Int, String)]
    			.rdd.collectAsMap()
    			.foreach{case (colorId, colorName) =>
			        if(null == colorCol){
				        colorCol = when($"ogcolor".equalTo(colorName), colorId)
			        }else{
				        colorCol = colorCol.when($"ogcolor".equalTo(colorName), colorId)
			        }
			    }
			// 设置other wise
			colorCol = colorCol.otherwise(0).as("color")
			// 返回Column对象
			colorCol
		}
		
		// 2.3 加载商品维度表数据
		val productsDF: DataFrame = {
			spark.read
				.format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url",
					"jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
						"useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
				.option("dbtable", "profile_tags.tbl_dim_products")
				.option("user", "root")
				.option("password", "123456")
				.load()
		}

		// 2.4. 构建颜色WHEN语句
		var productColumn: Column = {
			// 声明变量
			var productCol: Column = null
			productsDF
				.as[(Int, String)].rdd
				.collectAsMap()
				.foreach{case (productId, prodcutName) =>
					if(null == productCol){
						productCol = when($"producttype".equalTo(prodcutName), productId)
					}else{
						productCol = productCol.when($"producttype".equalTo(prodcutName), productId)
					}
				}
			productCol = productCol.otherwise(0).as("product")
			// 返回
			productCol
		}
		
		// 2.5. 根据运营规则标注的部分数据
		val labelColumn: Column = {
			when($"ogcolor".equalTo("樱花粉")
				.or($"ogcolor".equalTo("白色"))
				.or($"ogcolor".equalTo("香槟色"))
				.or($"ogcolor".equalTo("香槟金"))
				.or($"productType".equalTo("料理机"))
				.or($"productType".equalTo("挂烫机"))
				.or($"productType".equalTo("吸尘器/除螨仪")), 1) //女
				.otherwise(0)//男
				.alias("label")//决策树预测label
		}
		
		// 3. 商品数据和订单数据关联
		val genderDF: DataFrame = businessDF
			// 对业务数据进行转换
			.select(
				$"cordersn".as("ordersn"), // 为了后续关联使用方便
				colorColumn, // 将颜色转换为id
				productColumn, // 将产品名称转换为类别ID
				labelColumn // 标注数据，给用户的每个商品打上性别
			)
			// 依据订单号关联
    		.join(ordersDF, "ordersn")
			// 选取字段
    		.select(
			    $"memberid".as("uid"),
			    $"label", $"color", $"product"
		    )
		
		// 4. 获取性别信息，进行打标签
		//val predictionDF: DataFrame = genderDF.select($"uid", $"label".as("prediction"))
		//predictionDF.printSchema()
		//predictionDF.show(100, truncate = false)
		
		// ===================================================================
		// TODO: i. 构建决策树分类模型，使用算法模型预测用户购物商品性别
		//val featuresDF: DataFrame = featuresTransform(genderDF)
		//val dtcModel: DecisionTreeClassificationModel = trainModel(featuresDF)
		//val predictionDF: DataFrame = dtcModel.transform(featuresDF)
		
		// TODO: ii. 构建Pipeline模型，封装特征转换和训练
		//val pipelineModel: PipelineModel = trainPipelineModel(genderDF)
		//val predictionDF: DataFrame = pipelineModel.transform(genderDF)
		
		// TODO: iii. 使用TrainValidationSplit，调整超参数，获取最佳模型
		//val pipelineModel: PipelineModel = trainBestModel(genderDF)
		//val predictionDF: DataFrame = pipelineModel.transform(genderDF)
		
		// TODO: iv. 加载模型，当模型已经存在，直接加载获取使用；如果不存在，需要获取最佳模型并保存
		val modelPath = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"
		val pipelineModel: PipelineModel = MLModelTools.loadModel(
			genderDF, "usg", modelPath
		).asInstanceOf[PipelineModel]
		val predictionDF: DataFrame = pipelineModel.transform(genderDF)
		
		// ===================================================================
		predictionDF.printSchema()
		predictionDF.show(100, truncate = false)
		
		// 5. 计算每个用户所有购买商品中，男、女商品占比
		val usgDF: DataFrame = predictionDF
			.select(
				$"uid", //
				// 当prediction为0时，表示男性商品
				when($"prediction" === 0, 1)
					.otherwise(0).as("male"), // 男
				// 当prediction为1时，表示女性商品
				when($"prediction" === 1, 1)
					.otherwise(0).as("female") // 女
			)
			.groupBy($"uid")
			.agg(
				count($"uid").as("total"), // 用户购物次数
				sum($"male").as("maleTotal"), //预测为男性的次数
				sum($"female").as("femaleTotal") //预测为女性的次数
			)
		
		// 6. 直接获取属性标签规则rule
		val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
		// 采用广播变量广播
		val ruleMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(ruleMap)
		
		// 7. 自定义UDF函数，计算每个用户占比
		val gender_tag_udf: UserDefinedFunction = udf(
			(total: Long, maleTotal: Double, femaleTotal: Double) => {
				// 计算男、女性商品占比
				val maleRate = maleTotal / total
				val femaleRate = femaleTotal / total
				// 规则判断
				if(maleRate >= 0.6){
					ruleMapBroadcast.value("0")
				}else if(femaleRate >= 0.6){
					ruleMapBroadcast.value("1")
				}else{
					ruleMapBroadcast.value("-1")
				}
			}
		)
		
		// 8. 打标签，给每个用户打上购物性别标签ID
		val modelDF: DataFrame = usgDF.select(
			$"uid", //
			gender_tag_udf($"total", $"maleTotal", $"femaleTotal").as("tagId")
		)
		modelDF.show(100, truncate = false)
		
		// 9. 返回模型标签数据
		modelDF
	}
	
	/**
	 * 针对数据集进行特征工程：特征提取、特征转换及特征选择
	 * @param dataframe 数据集
	 * @return 数据集，包含特征列features: Vector类型和标签列label
	 */
	def featuresTransform(dataframe: DataFrame): DataFrame = {
		// a. 特征向量化
		val assembler: VectorAssembler = new VectorAssembler()
			.setInputCols(Array("color", "product"))
			.setOutputCol("raw_features")
		val df1: DataFrame = assembler.transform(dataframe)
		
		// b. 类别特征进行索引
		val vectorIndexer: VectorIndexerModel = new VectorIndexer()
			.setInputCol("raw_features")
			.setOutputCol("features")
			.setMaxCategories(30)
			.fit(df1)
		val df2: DataFrame = vectorIndexer.transform(df1)
		
		// c. 返回特征数据
		df2
	}
	
	/**
	 * 使用决策树分类算法训练模型，返回DecisionTreeClassificationModel模型
	 */
	def trainModel(dataframe: DataFrame): DecisionTreeClassificationModel = {
		// a. 数据划分为训练数据集和测试数据集
		val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2), seed = 123)
		
		// b. 构建决策树分类器
		val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
			.setFeaturesCol("features")
			.setLabelCol("label")
			.setPredictionCol("prediction")
			.setMaxDepth(5) // 树的深度
			.setMaxBins(32) // 树的叶子数目
			.setImpurity("gini") // 基尼系数
		
		// c. 训练模型
		logWarning("正在训练模型...................................")
		val dtcModel: DecisionTreeClassificationModel = dtc.fit(testingDF)
		
		// d. 模型评估
		val predictionDF: DataFrame = dtcModel.transform(testingDF)
		predictionDF.select("label", "prediction").show(100, truncate = false)
		val accuracy = modelEvaluate(predictionDF, "accuracy")
		println(s"Accuracy = $accuracy")
		
		// e. 返回模型
		dtcModel
	}
	
	
	/**
	 * 模型评估，返回计算分类指标值
	 *
	 * @param dataframe 预测结果的数据集
	 * @param metricName 分类评估指标名称，支持：f1、weightedPrecision、weightedRecall、accuracy
	 */
	def modelEvaluate(dataframe: DataFrame, metricName: String): Double = {
		// a. 构建多分类分类器
		val evaluator = new MulticlassClassificationEvaluator()
			.setLabelCol("label")
			.setPredictionCol("prediction")
			// 指标名称，
			.setMetricName(metricName)
		// b. 计算评估指标
		val metric: Double = evaluator.evaluate(dataframe)
		// c. 返回指标
		metric
	}
	
	
	/**
	 * 使用决策树分类算法训练模型，返回PipelineModel模型
	 *
	 * @return
	 */
	def trainPipelineModel(dataframe: DataFrame): PipelineModel = {
		// 1. 数据划分为训练数据集和测试数据集
		val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2), seed = 123)
		
		// 2. 构建Pipeline管道
		// 2.1. 特征向量化
		val assembler: VectorAssembler = new VectorAssembler()
			.setInputCols(Array("color", "product"))
			.setOutputCol("raw_features")
		
		// 2.2. 类别特征进行索引
		val vectorIndexer: VectorIndexer = new VectorIndexer()
			.setInputCol("raw_features")
			.setOutputCol("features")
			.setMaxCategories(30)
		
		// 2.3. 构建决策树分类器
		val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
			.setFeaturesCol("features")
			.setLabelCol("label")
			.setPredictionCol("prediction")
			.setMaxDepth(5) // 树的深度
			.setMaxBins(32) // 树的叶子数目
			.setImpurity("gini") // 基尼系数
		
		// 2.4. 设置Pipeline中对象Stage， TODO：Pipeline管道中既可以是转换器又可以是模型学习器（算法）
		val pipeline: Pipeline = new Pipeline()
			.setStages(Array(assembler, vectorIndexer, dtc))
		
		// 3. 使用数据训练模型, TODO: PipelineModel中Stage类型全部是转换器，只要传递DataFrame，经过一系列转换
		val pipelineModel: PipelineModel = pipeline.fit(trainingDF)
		val dtcModel = pipelineModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
		println(dtcModel.toDebugString)
		
		// 4. 使用管道模型预测
		val predictionDF: DataFrame = pipelineModel.transform(testingDF)
		predictionDF.printSchema()
		predictionDF.show(10, truncate = false)
		
		// 5. 模型评估
		val accuracy = modelEvaluate(predictionDF, "accuracy")
		println(s"Accuracy = $accuracy")
		
		// 6. 返回模型
		pipelineModel
	}
	
	/**
	 * 调整算法超参数，找出最优模型
	 * @param dataframe 数据集
	 * @return
	 */
	def trainBestModel(dataframe: DataFrame): PipelineModel = {
		// a. 特征向量化
		val assembler: VectorAssembler = new VectorAssembler()
			.setInputCols(Array("color", "product"))
			.setOutputCol("raw_features")
		
		// b. 类别特征进行索引
		val indexer: VectorIndexer= new VectorIndexer()
			.setInputCol("raw_features")
			.setOutputCol("features")
			.setMaxCategories(30)
		//	.fit(dataframe)
		
		// c. 构建决策树分类器
		val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
			.setFeaturesCol("features")
			.setLabelCol("label")
			.setPredictionCol("prediction")
		
		// d. 构建Pipeline管道流实例对象， TODO： 模型学习器【算法】
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
		
		// g. 训练验证
		val trainValidationSplit: TrainValidationSplit = new TrainValidationSplit()
			.setEstimator(pipeline) // 设置模型学习器，算法
			.setEvaluator(evaluator) // 设置模型评估器
			.setEstimatorParamMaps(paramGrid) // 算法超参数
			// 80% of the data will be used for training and the remaining 20% for validation.
			.setTrainRatio(0.8) // 训练集和验证集比例
		
		// h. 训练模型
		val model: TrainValidationSplitModel = trainValidationSplit.fit(dataframe)
		
		// i. 获取最佳模型返回
		model.bestModel.asInstanceOf[PipelineModel]
	}
	
}

object UsgTagModel{
	def main(args: Array[String]): Unit = {
		val tagModel = new UsgTagModel()
		tagModel.executeModel(378L)
	}
}