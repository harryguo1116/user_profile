package cn.itcast.tags.models.rule

import cn.itcast.tags.models._
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @Author Harry
 * @Date 2020-08-26 11:27
 * @Description   标签模型开发：国籍标签模型
 */
class NationalityModel extends AbstractModel("国籍标签",ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    /*
    332,国籍,
        333,中国大陆,1
        334,中国香港,2
        335,中国澳门,3
        336,中国台湾,4
     */
    TagTools.ruleMatchTag(businessDF,"nationality",tagDF)
  }
}

object NationalityModel {
  def main(args: Array[String]): Unit = {

    val nationalityModel = new NationalityModel()
    nationalityModel.executeModel(332L)
  }
}