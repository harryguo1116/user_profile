package cn.itcast.tags.meta

import cn.itcast.tags.utils.DateUtils

/**
 * HBase 元数据解析存储，具体数据字段格式如下所示：
 * inType=hbase
 * zkHosts=bigdata-cdh01.itcast.cn
 * zkPort=2181
 * hbaseTable=tbl_tag_users
 * family=detail
 * selectFieldNames=id,gender
 * whereCondition=modified#day#30
 */
case class HBaseMeta(
	                    zkHosts: String,
	                    zkPort: String,
	                    hbaseTable: String,
	                    family: String,
	                    selectFieldNames: String ,
											filterConditions: String
                    )

object HBaseMeta{
	
	/**
	 * 将Map集合数据解析到HBaseMeta中
	 * @param ruleMap map集合
	 */
	def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
		// TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略

		//whereCondition=modified#day#30
		// filterConditions: modified[lt]2020-07-07,modified[gt]2020-08-06
		//1. 获取设置where条件值
		val whereCondition: String = ruleMap.getOrElse("whereCondition",null)
		//2. 解析where条件值 组装为filterConditions字符串
		val filterConditions:String = if(null != whereCondition ){
			//1. 使用分隔符进行分割
			val Array(field,unit,amount) = whereCondition.split("#")
			//2. 获取昨日日期
			val nowDate = DateUtils.getNow()
			val yesterdayDate = DateUtils.dateCalculate(nowDate,-1)
			//3. 获取开始日期
			val agoDate = unit.toLowerCase match {
				case "day" => DateUtils.dateCalculate(yesterdayDate,-amount.toInt)
				case "month" => DateUtils.dateCalculate(yesterdayDate,-(amount.toInt*30))
				case "year" => DateUtils.dateCalculate(yesterdayDate,-(amount.toInt*365))
			}
			//4. 拼接字符串
			s"$field[ge]$agoDate,$field[le]$yesterdayDate"
		}else null

		HBaseMeta(
			ruleMap("zkHosts"),
			ruleMap("zkPort"),
			ruleMap("hbaseTable"),
			ruleMap("family"),
			ruleMap("selectFieldNames"),
			filterConditions
		)
	}
}