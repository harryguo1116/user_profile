package cn.itcast.tags.test.config

import java.util
import java.util.Map

import com.typesafe.config.{ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf

/**
 * @Author Harry
 * @Date 2020-08-25 20:27
 * @Description 测试属性文件的配置
 */
object SparkConfigTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //使用ConfigFactory加载spark.conf
    val config = ConfigFactory.load("spark.conf")
    //获取加载配置信息
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    // 遍历数据
    import scala.collection.JavaConverters._
    for (entry <- entrySet.asScala){
      // 获取属性来源的文件名称
      val resourceStr = entry.getValue.origin().resource()
      if("spark.conf".equals(resourceStr)){
        println(entry.getKey+":"+entry.getValue.unwrapped().toString)
      }
    }
  }
}
