package cn.itcast.tags.models.statistics

import cn.itcast.tags.models._
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * @Author Harry
 * @Date 2020-08-26 21:25
 * @Description 标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签", ModelType.STATISTICS) {
  /*
    356 支付方式
      357 支付宝 alipay
      358 微信支付 wxpay
      359 银联支付 chinapay
      360 货到付款 cod
  */

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._

    val paymentDF: DataFrame = businessDF
      // 按照会员ID和支付编码分组，统计次数
      .groupBy($"memberid", $"paymentcode")
      .count()
      // 获取每个会员支付方式最多，使用开窗排序函数ROW_NUMBER
      .withColumn(
        "rnk",
        row_number().over(
          Window.partitionBy($"memberid").orderBy($"count".desc)
        )
      )
      .where($"rnk".equalTo(1))
      .select($"memberid".as("id"), $"paymentcode")
    println("==============paymentDF=============")
    paymentDF.show()
    val modelDF: DataFrame = TagTools.ruleMatchTag(
      paymentDF, "paymentcode", tagDF
    )
    modelDF
  }
}

object PayTypeModel {
  def main(args: Array[String]): Unit = {

    val payTypeModel = new PayTypeModel()
    payTypeModel.executeModel(356L)
  }
}