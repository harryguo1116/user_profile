package cn.itcast.tags.meta

import org.apache.spark.sql.Column

/**
 * 从Json格式文件中加载数据，SparkSession创建时与Json集成已配置
			inType=Json
			inPath=/apps/datas/tbl_logs
			sperator= ()
			selectFieldNames=global_user_id,loc_url,log_time
 */
case class JsonMeta(
										 inPath: String,
										 sperator:String,
										 selectFieldNames: Array[Column]
                   )

object JsonMeta{
	/**
	 * 将Map集合数据解析到HdfsMeta中
	 * @param ruleMap map集合
	 * @return
	 */
	def getJsonMeta(ruleMap: Map[String, String]): JsonMeta = {

		// 将选择字段构建为Column对象
		import org.apache.spark.sql.functions._
		val fieldColumns: Array[Column] = ruleMap("selectFieldNames")
			.split(",")
			.map{field => col(field)}

		// 创建HdfsMeta对象并返回
		JsonMeta(
			ruleMap("inPath"),
			ruleMap("sperator"),
			fieldColumns
		)
	}
}

