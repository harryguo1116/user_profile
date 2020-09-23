package cn.itcast.tags.utils

import java.util
import java.util.Map

import cn.itcast.tags.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author Harry
 * @Date 2020-08-25 20:50
 * @Description   创建SparkSession对象工具类
 */
object SparkUtils {

  /**
   *  加载Spark Application默认配置文件，设置到SparkConf中
   *  @param resource 资源配置文件名称
   *  @return SparkConf对象
   */
  def loadConf (resource:String):SparkConf = {

    //1. 创建SparkConf对象
    val sparkConf = new SparkConf()

    //2. 使用ConfigFactory 创建config
    val config: Config = ConfigFactory.load(resource)

    //3. 获取配置信息的内容
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    import scala.collection.JavaConverters._
    for(entry <- entrySet.asScala){
      val resourceName: String = entry.getValue.origin().resource()
      if(resource.equals(resourceName)){
        sparkConf.set(entry.getKey,entry.getValue.unwrapped().toString)
      }
    }
    sparkConf
  }

  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   * @return
   */
  def createSparkSession(clazz:Class[_],isHive:Boolean = false):SparkSession = {
    //1. 构建SparkSession对象
    val sparkConf: SparkConf = loadConf(resource = "spark.properties")
    //2. 判断是否为本地模式
    if(ModelConfig.APP_IS_LOCAL){
      sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
    }
    // 3. 创建SparkSession.Builder对象
    var builder: SparkSession.Builder = SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)
    // 4. 判断应用是否集成Hive，如果集成，设置Hive MetaStore地址
    // 如果在config.properties中设置集成Hive，表示所有SparkApplication都集成Hive；
    // 否则判断isHive，表示针对某个具体应用是否集成Hive
    if(ModelConfig.APP_IS_HIVE || isHive){
      builder = builder
        .config("hive.metastore.uris",
          ModelConfig.APP_HIVE_META_STORE_URL)
        .enableHiveSupport()
    }

    // 5. 获取SparkSession对象
    val session = builder.getOrCreate()
    // 6. 返回
    session

  }
}