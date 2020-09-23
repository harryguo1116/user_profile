package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractTagModel, ModelType}
import cn.itcast.tags.tools.{MLModelTools, TagTools}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @Author Harry
 * @Date 2020-08-30 21:38
 * @Description 挖掘类型标签模型开发：用户价格敏感度标签模型（PSM模型）
 */
class PsmModel extends AbstractTagModel("价格敏感度标签", ModelType.ML) {

  /*
  372,消费敏感度
    373,极度敏感,   0
    374,比较敏感,   1
    375,一般敏感,   2
    376,不太敏感,   3
    377,极度不敏感,  4

   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

    val spark: SparkSession = businessDF.sparkSession
    import spark.implicits._

    // 计算指标
    //ra: receivableAmount 应收金额
    val raColumn: Column = ($"orderamount" + $"couponcodevalue").as("ra")
    //da: discountAmount 优惠金额
    val daColumn: Column = $"couponcodevalue".as("da")
    //pa: practicalAmount 实收金额
    val paColumn: Column = $"orderamount".as("pa")
    //state: 订单状态，此订单是否是优惠订单，0表示非优惠订单，1表示优惠订单
    val stateColumn: Column = when($"couponcodevalue" === 0.0, 0)
      .otherwise(1).as("state")

    //tdon 优惠订单数
    val tdonColumn: Column = sum($"state").as("tdon")
    //ton  总订单总数
    val tonColumn: Column = count($"state").as("ton")
    //tda 优惠总金额
    val tdaColumn: Column = sum($"da").as("tda")
    //tra 应收总金额
    val traColumn: Column = sum($"ra").as("tra")

    /*
      tdonr 优惠订单占比(优惠订单数 / 订单总数)
      tdar  优惠总金额占比(优惠总金额 / 订单总金额)
      adar  平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
    */
    val tdonrColumn: Column = ($"tdon" / $"ton").as("tdonr")
    val tdarColumn: Column = ($"tda" / $"tra").as("tdar")
    val adarColumn: Column = (
      ($"tda" / $"tdon") / ($"tra" / $"ton")
      ).as("adar")
    val psmColumn: Column = ($"tdonr" + $"tdar" + $"adar").as("psm")

    // 1. 依据订单数据，计算PSM值
    val psmDF: DataFrame = businessDF
      .select(
        $"memberid".as("uid"), //
        daColumn, raColumn, stateColumn
      )
      // 按照用户ID分组，聚合操作：订单数和订单金额
      .groupBy($"uid")
      .agg(
        tdonColumn, tonColumn, tdaColumn, traColumn
      )
      // 计算各个优惠占比
      .select(
        $"uid", //
        tdonrColumn, tdarColumn, adarColumn //
      )
      // 计算PSM值
      .select($"*", psmColumn)
      // TODO：如果某人订单没有使用任何优惠，此人计算平均优惠金额占比为null，所有PSM为null，转换为数值类型
      .select(
        $"*", //
        when($"psm".isNull, 0.00000001)
          .otherwise($"psm").as("psm_score")
      )
    psmDF.show(10,false)
    // 2. 使用KMeans算法训练模型（最佳模型）
    // 2.1 组合特征数据为vector
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("psm_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(psmDF)
    // 2.2 加载算法模型，先构建最佳算法模型存储，运行标签模型应用时直接加载，进行预测
    val modelPath: String = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"
    val kmeansModel: KMeansModel = MLModelTools.loadModel(
      featuresDF, "psm", modelPath
    ).asInstanceOf[KMeansModel]
    // 2.3 使用算法模型预测数据类簇
    val predictionDF: DataFrame = kmeansModel.transform(featuresDF)

    // 3. 模型预测类簇、属性标签数据规则给每个用户打标签
    val modelDF: DataFrame = TagTools.kmeansMatchTag(
      kmeansModel, predictionDF, tagDF
    )
    //    modelDF.show(100, truncate = false)
    // 4. 返回标签模型数据
    modelDF
  }
}

object PsmModel {
  def main(args: Array[String]): Unit = {

    val model = new PsmModel()
    model.executeModel(372L)
  }
}
