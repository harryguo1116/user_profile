package cn.itcast.tags.models.rule

import cn.itcast.tags.models._
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @Author Harry
 * @Date 2020-08-26 11:33
 * @Description 标签模型开发：政治面貌标签模型
 */
class PoliticalModel extends AbstractModel("政治面貌标签", ModelType.MATCH) {
  /*
  328,政治面貌, field: politicalface
      329,群众,1
      330,党员,2
      331,无党派人士,3

   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    TagTools.ruleMatchTag(businessDF, "politicalface", tagDF)

  }
}

object PoliticalModel {
  def main(args: Array[String]): Unit = {

    val politicalModel = new PoliticalModel()
    politicalModel.executeModel(328L)
  }
}
