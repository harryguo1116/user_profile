package cn.itcast.tags.test.hbase.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示：SparkCore如何从HBase表读取数据
 */
object HBaseReadTest {
	
	def main(args: Array[String]): Unit = {
		
		// 创建SparkContext实例对象
		val sparkConf = new SparkConf()
			.setMaster("local[4]")
			.setAppName("HBaseReadTest")
			// 设置使用Kryo序列
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
			.registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
		val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
		
		// TODO: 从HBase表加载数据，封装到RDD中
		/*
		  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
		      conf: Configuration = hadoopConfiguration,
		      fClass: Class[F],
		      kClass: Class[K],
		      vClass: Class[V]
		   ): RDD[(K, V)]
		 */
		// 1. 创建Configuration对象，进行相关设置
		val conf: Configuration = HBaseConfiguration.create()
		// 1.a 设置HBase依赖Zookeeper
		conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("zookeeper.znode.parent", "/hbase")
		// 1.b 设置读取表的名称
		conf.set(TableInputFormat.INPUT_TABLE, "tbl_tag_users")
		// 2. 调用底层InputFormat加载HBase表的数据
		val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
			conf, //
			classOf[TableInputFormat], //
			classOf[ImmutableBytesWritable], //
			classOf[Result] //
		)
		println(s"Count = ${datasRDD}")
		
		datasRDD.take(1).foreach{case (_, result) =>
			println(s"RowKey = ${Bytes.toString(result.getRow)}")
			result.rawCells().foreach{cell =>
				println(s"\t${Bytes.toString(CellUtil.cloneFamily(cell))}:${Bytes.toString(CellUtil.cloneQualifier(cell))}=${Bytes.toString(CellUtil.cloneValue(cell))}")
			}
		}
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
