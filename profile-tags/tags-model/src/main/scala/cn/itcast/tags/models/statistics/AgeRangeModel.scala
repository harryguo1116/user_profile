package cn.itcast.tags.models.statistics

import cn.itcast.tags.models._
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * @Author Harry
 * @Date 2020-08-26 15:32
 * @Description 标签模型开发：年龄段标签模型
 */
class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {

  /*
    338 年龄段
      属性标签数据：
      339 50后 19500101-19591231
      340 60后 19600101-19691231
      341 70后 19700101-19791231
      342 80后 19800101-19891231
      343 90后 19900101-19991231
      344 00后 20000101-20091231
      345 10后 20100101-20191231
      346 20后 20200101-20291231
    业务数据：
      99 column=detail:birthday, value=1982-01-11 -> 19820111
    分析思路：
      比较birthday日期 在 某个年龄段之内，给予标签ID
    19820111 -> 342
      实现：JOIN，UDF函数
*/
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

    import businessDF.sparkSession.implicits._
    val attrTagDF:DataFrame= TagTools.convertTuple(tagDF)
//    attrTagDF.show()

    /**
     * +-----+--------+--------+
     * |tagId|   start|     end|
     * +-----+--------+--------+
     * |  339|19500101|19591231|
     * |  340|19600101|19691231|
     * +-----+--------+--------+
     */

    // 3. 业务数据与标签规则关联JOIN，比较范围
    /*
      attrTagDF： attr
      businessDF: business
      SELECT t2.userId, t1.tagId FROM attr t1 JOIN business t2
      WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
    */
    // 3.1. 转换日期格式： 1982-01-11 -> 19820111
    /**
     * +---+----------+
     * | id|  birthday|
     * +---+----------+
     * |  1|1992-05-31|
     * | 10|1980-10-13|
     * +---+----------+
     */
    val birthdayDF: DataFrame = businessDF
      .select(
        $"id".as("uid"),
        regexp_replace($"birthday", "-", "")
          .cast(IntegerType).as("bronDate")
      )
    val modelDF: DataFrame = birthdayDF.join(attrTagDF)
      //设置关联条件 出生日期在开始和结束之间
      .where(birthdayDF("bronDate").between(attrTagDF("start"), attrTagDF("end")))
      .select(
        $"uid", $"tagId".cast(StringType)
      )
    //modelDF.show()
    modelDF
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    val ageRangeModel = new AgeRangeModel()
    ageRangeModel.executeModel(338L)
  }
}
