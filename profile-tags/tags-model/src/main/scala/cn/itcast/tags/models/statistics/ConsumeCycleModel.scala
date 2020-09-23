package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/**
 * @Author Harry
 * @Date 2020-08-26 18:58
 * @Description 标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS) {
  /*
  347 消费周期
      348 近7天 0-7
      349 近2周 8-14
      350 近1月 15-30
      351 近2月 31-60
      352 近3月 61-90
      353 近4月 91-120
      354 近5月 121-150
      355 近半年 151-180
  */

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val attrTagDF: DataFrame = TagTools.convertTuple(tagDF)
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //    println("==========tupleDF===========")
    //    tupleDF.show()


    /**
     * +-----+-----+---+
     * |tagId|start|end|
     * +-----+-----+---+
     * |  348|    0|  7|
     * |  349|    8| 14|
     * |  350|   15| 30|
     * +-----+-----+---+
     */
    // 把businessDF的完成时间格式进行转化
    /** businessDF
     * +---------+----------+
     * | memberid|finishtime|
     * +---------+----------+
     * | 13823431|1564415022|
     * |  4035167|1565687310|
     * |  4035291|1564681801|
     * |  4035041|1565799378|
     * +---------+----------+
     */

    // 对用户进行分组，求订单完成时间的最大值  即最近消费的时间
    val daysDF: DataFrame = businessDF
      .groupBy($"memberid")
      .agg(
        from_unixtime(max($"finishtime")).as("finish_time")
      )
      // 最近消费的时间和当前时间求取时间差
      .select(
        $"memberid".as("uid"),
        datediff(current_date(), $"finish_time").as("consumer_days")
      )
    // 把差值的数据和原来数据进行join
    val modelDF: DataFrame = daysDF.join(attrTagDF)
      .where(daysDF("consumer_days").between(attrTagDF("start"), attrTagDF("end")))
      .select($"uid", $"tagId".cast(StringType))

    modelDF
  }
}

object ConsumeCycleModel {
  def main(args: Array[String]): Unit = {

    val consumeCycleModel = new ConsumeCycleModel()
    consumeCycleModel.executeModel(347L)
  }
}
