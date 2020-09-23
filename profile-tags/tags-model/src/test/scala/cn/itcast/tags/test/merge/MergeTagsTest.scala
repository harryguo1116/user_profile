package cn.itcast.tags.test.merge

/**
 * @Author Harry
 * @Date 2020-08-26 20:44
 * @Description  测试优化标签
 */
object MergeTagsTest {
  def main(args: Array[String]): Unit = {

    // 画像标签
    val tagIds: String = "319,322,329,333,354"
    // 计算标签
    val tagId: String = "348"
    // 消费周期所有标签
    val ids: Set[String] = Set("348", "349", "350", "351", "352",
      "353", "354", "355")
    // 画像标签Set集合
    var tagIdsSet: Set[String] = tagIds.split(",").toSet
    println(tagIdsSet.mkString("-"))
    // 取交集
    val interSet: Set[String] = tagIdsSet & ids
    println(interSet.mkString("--"))
    // 先去掉交集的数据 再合并新标签
    val newTagIds: Set[String] = tagIdsSet -- interSet + tagId
    println(newTagIds.mkString(","))
  }

}
