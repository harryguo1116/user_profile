package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.{MLModelTools, TagTools}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @Author Harry
 * @Date 2020-08-30 14:26
 * @Description 开发标签模型（挖掘类型标签）：用户活跃度RFE模型
 */

class RfeTagModel extends AbstractModel("用户活跃度RFE", ModelType.ML) {
  /*
    367 用户活跃度
       368,非常活跃,  0
       369,活跃,     1
       370,不活跃,   2
       371,非常不活跃,3

  */

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    /*
    root
     |-- global_user_id: string (nullable = true)
     |-- loc_url: string (nullable = true)
     |-- log_time: string (nullable = true)
     */
    //businessDF.printSchema()
    /*
      +--------------+-------------------------------------------------------------------+-------------------+
      |global_user_id|loc_url                                                            |log_time           |
      +--------------+-------------------------------------------------------------------+-------------------+
      |424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377    |2019-08-13 03:03:55|
      |619           |http://m.eshop.com/?source=mobile                                  |2019-07-29 15:07:41|
      |898           |http://m.eshop.com/mobile/item/11941.html                          |2019-08-14 09:23:44|
      |642           |http://www.eshop.com/l/2729-2931.html                              |2019-08-11 03:20:17|
      |130           |http://www.eshop.com/                                              |2019-08-12 11:59:28|
      |515           |http://www.eshop.com/l/2723-0-0-1-0-0-0-0-0-0-0-0.html             |2019-07-23 14:39:25|
      |274           |http://www.eshop.com/                                              |2019-07-24 15:37:12|
      |772           |http://ck.eshop.com/login.html                                     |2019-07-24 07:56:49|
      |189           |http://m.eshop.com/mobile/item/9673.html                           |2019-07-26 19:17:00|
      |529           |http://m.eshop.com/mobile/search/_bplvbiwq_XQS75_btX_ZY1328-se.html|2019-07-25 23:18:37|
      +--------------+-------------------------------------------------------------------+-------------------+
     */
    //businessDF.show(10, truncate = false)

    val spark: SparkSession = businessDF.sparkSession
    import spark.implicits._

    // 1. 依据用户行为日志数据（流量数据）计算每个用户的RFE值
    val rfeDF: DataFrame = businessDF
      .groupBy($"global_user_id")
      .agg(
        max($"log_time").as("last_time"), //
        count($"loc_url").as("frequency"), //
        countDistinct($"loc_url").as("engagements")
      )
      .select(
        $"global_user_id".as("uid"), //
        // 计算R值
        datediff(
          date_sub(current_timestamp(), 370), $"last_time"//
        ).as("recency"), //
        $"frequency", $"engagements" //
      )

    // 2. 按照规则，给用户RFE值进行打分Score
    /*
      R：0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
      F：≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
      E：≥250=5分，200-249=4分，150-199=3分，149-50=2分，≤49=1分
     */
    // R 打分条件表达式
    val rWhen = when(col("recency").between(1, 15), 5.0) //
      .when(col("recency").between(16, 30), 4.0) //
      .when(col("recency").between(31, 45), 3.0) //
      .when(col("recency").between(46, 60), 2.0) //
      .when(col("recency").geq(61), 1.0) //
    // F 打分条件表达式
    val fWhen = when(col("frequency").leq(99), 1.0) //
      .when(col("frequency").between(100, 199), 2.0) //
      .when(col("frequency").between(200, 299), 3.0) //
      .when(col("frequency").between(300, 399), 4.0) //
      .when(col("frequency").geq(400), 5.0) //
    // M 打分条件表达式
    val eWhen = when(col("engagements").lt(49), 1.0) //
      .when(col("engagements").between(50, 149), 2.0) //
      .when(col("engagements").between(150, 199), 3.0) //
      .when(col("engagements").between(200, 249), 4.0) //
      .when(col("engagements").geq(250), 5.0) //
    val rfeScoreDF: DataFrame = rfeDF.select(
      $"uid", rWhen.as("r_score"), //
      fWhen.as("f_score"), eWhen.as("e_score") //
    )
    /*
      root
       |-- uid: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- e_score: double (nullable = true)
     */
    //rfeScoreDF.printSchema()
    /*
       +---+-------+-------+-------+
      |uid|r_score|f_score|e_score|
      +---+-------+-------+-------+
      |1  |5.0    |5.0    |5.0    |
      |102|5.0    |5.0    |5.0    |
      |107|5.0    |5.0    |5.0    |
      |110|5.0    |4.0    |5.0    |
      |111|5.0    |4.0    |5.0    |
      |120|5.0    |4.0    |5.0    |
      |130|5.0    |5.0    |4.0    |
      |135|5.0    |4.0    |4.0    |
      |137|5.0    |5.0    |5.0    |
      |139|5.0    |4.0    |5.0    |
      +---+-------+-------+-------+
     */
    //rfeScoreDF.show(10, truncate = false)

    // 3. 使用KMeans算法构建算法模型（最佳模型：仅仅调整maxIter, 此处K=4）
    // 3.1 组合特征数据为vector
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "e_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(rfeScoreDF)

    // 3.2 加载算法模型，先构建最佳算法模型存储，运行标签模型应用时直接加载，进行预测
    val modelPath = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"
    val kmeansModel: KMeansModel = MLModelTools.loadModel(
      featuresDF, "rfe", modelPath
    ).asInstanceOf[KMeansModel]
    val predictionDF: DataFrame = kmeansModel.transform(featuresDF)

    // 4. 使算法模型及属性标签数据，给每个用户打标签
    val modelDF: DataFrame = TagTools.kmeansMatchTag(
      kmeansModel, predictionDF, tagDF
    )

    //modelDF.printSchema()
    //modelDF.show(100, truncate = false)

    // 返回标签模型数据
    modelDF
  }
}

object RfeTagModel {
  def main(args: Array[String]): Unit = {
    val model = new RfeTagModel()
    model.executeModel(367L)
  }
}

