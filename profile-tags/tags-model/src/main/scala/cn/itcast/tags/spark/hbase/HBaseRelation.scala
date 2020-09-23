package cn.itcast.tags.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
 * @Author Harry
 * @Date 2020-08-26 13:30
 * @Description 自定义外部数据源：从HBase表加载数据和保存数据值HBase表的Relation实现
 */
case class HBaseRelation(context: SQLContext, params: Map[String, String], userSchema: StructType)
  extends BaseRelation with TableScan with InsertableRelation with Serializable {

  //连接HBase数据库的属性名称
  val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  val HBASE_ZK_PORT_VALUE: String = "zkPort"
  val HBASE_TABLE: String = "hbaseTable"
  val HBASE_TABLE_FAMILY: String = "family"
  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

  // filterConditions：modified[GE]20190601,modified[LE]20191201
  val HBASE_TABLE_FILTER_CONDITIONS: String = "filterConditions"
  /**
   * SQLContext实例对象
   */
  override def sqlContext: SQLContext = context

  /**
   * DataFrame的Schema信息
   */
  override def schema: StructType = userSchema

  /**
   * 如何从HBase表中读取数据，返回RDD[Row]
   */
  override def buildScan(): RDD[Row] = {

    //1. 设置HBase中Zookeeper集群信息
    val conf = new Configuration()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))

    //2. 设置读取HBase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
    //3. 设置列簇和列名称
    val scan: Scan = new Scan()
    //3.1 设置列簇
    val familyBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    scan.addFamily(familyBytes)
    //3.2 设置列名称
    val fields = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
    fields.foreach { field =>
      scan.addColumn(familyBytes, Bytes.toBytes(field))
    }
    // TODO: 添加要设置过滤器Filter，比如依据某个字段的值进行过滤操作，
    // ===================================================================
    // step 1：获取过滤条件值
    val filterConditions: String = params.getOrElse(HBASE_TABLE_FILTER_CONDITIONS, null)
    //step 2 : 判断过滤条件是否有值
    if(null != filterConditions && filterConditions.trim.split(SPERATOR).length > 0){
      val filterList = new FilterList()
      filterConditions
        .split(SPERATOR)
        .foreach{filterCondition =>
          // 1. 解析过滤条件 封装Condition对象中
          val condition = Condition.parseCondiction(filterCondition)
          // 2. 依据过滤条件 获取过滤器
          val filter = new SingleColumnValueFilter(
            familyBytes,
            Bytes.toBytes(condition.field),
            condition.compare,
            Bytes.toBytes(condition.value)
          )
          //3. 将过滤器Filter加入到List中
          filterList.addFilter(filter)
          //4. todo 当使用过滤器时，确定过滤字段一定要读取值
          scan.addColumn(familyBytes,Bytes.toBytes(condition.field))
        }
      //step 3 : 设置过滤器
      scan.setFilter(filterList)
    }
    //=========================================================================
    //3.3 设置scan过滤
    conf.set(
      TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )
    //4. 调用底层API 读取HBase表数据
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext
      .newAPIHadoopRDD(
        conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
    //5. 转换为RDD[Row]
    val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
      //5.1 列的值
      val values: Seq[String] = fields.map { field =>
        Bytes.toString(result.getValue(
          familyBytes, Bytes.toBytes(field)))
      }
      Row.fromSeq(values)
    }
    //6. 返回Row对象
    rowsRDD
  }

  /**
   * 将数据DataFrame写入到HBase表中
   *
   * @param data      数据集
   * @param overwrite 保存模式
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //1、 数据转换
    val columns: Array[String] = data.columns
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map { row =>
      // 获取RowKey
      val rowKey: String = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME))
      //构建Put对象
      val put = new Put(Bytes.toBytes(rowKey))
      //将每列数据加入Put对象中
      val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
      columns.foreach {column =>
        put.addColumn(
          familyBytes,
          Bytes.toBytes(column),
          Bytes.toBytes(row.getAs[String](column))
        )
      }
      //返回二元组
      (new ImmutableBytesWritable(put.getRow),put)
    }

    //2. 设置HBase中的Zookeeper集群信息
    val conf:Configuration = new Configuration()
    conf.set(HBASE_ZK_QUORUM_KEY,params(HBASE_ZK_QUORUM_VALUE))
    conf.set(HBASE_ZK_PORT_KEY,params(HBASE_ZK_PORT_VALUE))
    //3. 设置读HBase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))
    //4、 保存数据到表
    putsRDD.saveAsNewAPIHadoopFile(
      s"/apps/hbase/${params(HBASE_TABLE)}-" +
        System.currentTimeMillis(),
      classOf[ImmutableBytesWritable], //
      classOf[Put], //
      classOf[TableOutputFormat[ImmutableBytesWritable]], //
      conf //
    )
  }
}
