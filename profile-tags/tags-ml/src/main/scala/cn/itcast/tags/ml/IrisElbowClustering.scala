package cn.itcast.tags.ml

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable

/**
 * 针对鸢尾花数据集进行聚类，使用KMeans算法，采用肘部法则Elbow获取K的值
 */
object IrisElbowClustering {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.sql.shuffle.partitions", "4")
			.getOrCreate()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		// 1. 加载鸢尾花数据，使用libsvm格式
		val irisDF: DataFrame = spark.read
			.format("libsvm")
			.load("datas/iris_kmeans.txt")
		irisDF.persist(StorageLevel.MEMORY_AND_DISK).count()
		/*
		root
		|-- label: double (nullable = true)
		|-- features: vector (nullable = true)
		*/
		//irisDF.printSchema()
		//irisDF.show(10, truncate = false)
	
		// TODO: 2. 设置不同的K -> 2 to 6，训练模型，使用轮廓系数评估模型
		/*
		  setDefault(
			    k -> 2,
			    maxIter -> 20,
			    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
			    initSteps -> 2,
			    tol -> 1e-4
		    )
		 */
		val clusters: immutable.Seq[(Int, Double, String)] = (2 to 6).map{ k =>
			// i. 构建KMeans算法对象
			val kmeans: KMeans = new KMeans()
    			.setFeaturesCol("features")
    			.setPredictionCol("prediction")
				// 设置K值
    			.setK(k)
				// 设置迭代次数
    			.setMaxIter(20).setInitMode("k-means||")
				// 距离计算方式：'euclidean' and 'cosine'.
    			.setDistanceMeasure("cosine")
			
			// ii. 特征数据应用算法训练模型
			val model: KMeansModel = kmeans.fit(irisDF)
			
			// iii. 计算评估指标：sc 轮廓系数
			val predictions: DataFrame = model.transform(irisDF)
			val preResult: String = predictions
    			.groupBy($"prediction").count()
    			.select($"prediction", $"count")
    			.as[(Int, Long)]
				.rdd.collectAsMap().mkString(", ")
			val evaluator: ClusteringEvaluator = new ClusteringEvaluator()
    			.setFeaturesCol("features")
    			.setPredictionCol("prediction")
    			.setMetricName("silhouette")
    			.setDistanceMeasure("cosine")
			val silhouette: Double = evaluator.evaluate(predictions)
			//println(s"Silhouette with squared euclidean distance = $silhouette")
			
			// iv. 返回
			(k, silhouette, preResult)
		}
		
		// 打印值
		/*
		========================== 欧式距离 ==========================
			(2,0.8501515983265806,1 -> 97, 0 -> 53)
			(3,0.7342113066202725,2 -> 39, 1 -> 50, 0 -> 61)
			(4,0.6748661728223084,2 -> 28, 1 -> 50, 3 -> 43, 0 -> 29)
			(5,0.5593200358940349,2 -> 30, 4 -> 17, 1 -> 33, 3 -> 47, 0 -> 23)
			(6,0.5157126401818913,2 -> 47, 5 -> 18, 4 -> 13, 1 -> 19, 3 -> 23, 0 -> 30)
		
		========================== 余弦距离 ==========================
			(2,0.9579554849242657,1 -> 50, 0 -> 100)
			(3,0.7484647230660575,2 -> 46, 1 -> 50, 0 -> 54)
			(4,0.5754341193280768,2 -> 46, 1 -> 19, 3 -> 31, 0 -> 54)
			(5,0.6430770644178772,2 -> 23, 4 -> 22, 1 -> 50, 3 -> 28, 0 -> 27)
			(6,0.4512255960897416,2 -> 43, 5 -> 21, 4 -> 18, 1 -> 29, 3 -> 15, 0 -> 24)
		 */
		clusters.foreach(println)
	
		
		// 应用结束，关闭资源
	}
	
}
