package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum, _}

/**
 * 挖掘类型标签模型开发：用户购物性别标签模型开发（USG 模型）
 */
class UsgModel extends AbstractModel("用户购物性别标签", ModelType.ML){
	/*
	378	用户购物性别
		379	男		0
		380	女		1
		381	中性		-1
	 */
	override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
		
		val spark: SparkSession = businessDF.sparkSession
		import spark.implicits._
		
		/*
		root
		 |-- cordersn: string (nullable = true)
		 |-- ogcolor: string (nullable = true)
		 |-- producttype: string (nullable = true)
		 */
		//businessDF.printSchema()
		/*
			+----------------------+---------+-----------+
			|cordersn              |ogcolor  |producttype|
			+----------------------+---------+-----------+
			|jd_14091818005983607  |白色       |烤箱         |
			|jd_14091317283357943  |香槟金      |冰吧         |
			|jd_14092012560709235  |香槟金色     |净水机        |
			|rrs_15234137          |梦境极光【布朗灰】|烤箱         |
			|suning_790750687478116|梦境极光【卡其金】|4K电视       |
			|rsq_805093707860210   |黑色       |烟灶套系       |
		 */
//		println("===============businessDF===============")
//		businessDF.show(10, truncate = false)
		
		// 1. 获取订单表数据tbl_tag_orders，与订单商品表数据关联获取会员ID
		val ordersDF: DataFrame = spark.read
			.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_tag_orders")
			.option("family", "detail")
			.option("selectFields", "memberid,ordersn")
			.load()
//		println("===============ordersDF===============")
//		ordersDF.show(10, truncate = false)
		/*
		+--------+----------------------+
		|memberid|ordersn               |
		+--------+----------------------+
		|232     |gome_792756751164275  |
		|190     |jd_14090106121770839  |
		 */

		
		// 2. 加载维度表数据：tbl_dim_colors（颜色）、tbl_dim_products（产品）
		// 2.1 加载颜色维度表数据
		val colorsDF: DataFrame = {
			spark.read
				.format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url",
					"jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
						"useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
				.option("dbtable", "profile_tags.tbl_dim_colors")
				.option("user", "root")
				.option("password", "123456")
				.load()
		}
		/*
			root
			 |-- id: integer (nullable = false)
			 |-- color_name: string (nullable = true)
		 */
		//colorsDF.printSchema()
//		println("===============colorsDF===============")
//		colorsDF.show(30, truncate = false)
		/*
		+---+----------+
		|id |color_name|
		+---+----------+
		|1  |香槟金色    |
		|2  |黑色       |
		|3  |白色       |
		+---+----------+
		 */
		
		// 2.2 构建颜色WHEN语句
		val colorColumn: Column = {
			// 声明可变变量
			var colorCol: Column = null
			// 循环遍历
			colorsDF.as[(Int, String)]
    			.rdd.collectAsMap()
    			.foreach{case (colorId, colorName) =>
			        if(null == colorCol){
				        colorCol = when($"ogcolor".equalTo(colorName), colorId)
			        }else{
				        colorCol = colorCol.when($"ogcolor".equalTo(colorName), colorId)
			        }
			    }
			// 设置other wise
			colorCol = colorCol.otherwise(0).as("color")
			// 返回Column对象
			colorCol
		}
		
		// 2.3 加载商品维度表数据
		val productsDF: DataFrame = {
			spark.read
				.format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url",
					"jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
						"useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
				.option("dbtable", "profile_tags.tbl_dim_products")
				.option("user", "root")
				.option("password", "123456")
				.load()
		}
		/*
			root
			 |-- id: integer (nullable = false)
			 |-- product_name: string (nullable = true)
		 */
		//productsDF.printSchema()
//		println("===============productsDF===============")
//		productsDF.show(30, truncate = false)
		/*
		+---+------------+
		|id |product_name|
		+---+------------+
		|1  |4K电视        |
		|2  |Haier/海尔冰箱  |
		 */
		// 2.4. 构建颜色WHEN语句
		var productColumn: Column = {
			// 声明变量
			var productCol: Column = null
			productsDF
				.as[(Int, String)].rdd
				.collectAsMap()
				.foreach{case (productId, prodcutName) =>
					if(null == productCol){
						productCol = when($"producttype".equalTo(prodcutName), productId)
					}else{
						productCol = productCol.when($"producttype".equalTo(prodcutName), productId)
					}
				}
			productCol = productCol.otherwise(0).as("product")
			// 返回
			productCol
		}
		
		// 2.5. 根据运营规则标注的部分数据
		val labelColumn: Column = {
			when($"ogcolor".equalTo("樱花粉")
				.or($"ogcolor".equalTo("白色"))
				.or($"ogcolor".equalTo("香槟色"))
				.or($"ogcolor".equalTo("香槟金"))
				.or($"productType".equalTo("料理机"))
				.or($"productType".equalTo("挂烫机"))
				.or($"productType".equalTo("吸尘器/除螨仪")), 1) //女
				.otherwise(0)//男
				.alias("label")//决策树预测label
		}
		
		// 3. 商品数据和订单数据关联
		val genderDF: DataFrame = businessDF
			// 对业务数据进行转换
			.select(
				$"cordersn".as("ordersn"), // 为了后续关联使用方便
				colorColumn, // 将颜色转换为id
				productColumn, // 将产品名称转换为类别ID
				labelColumn // 标注数据，给用户的每个商品打上性别
			)
			// 依据订单号关联
    		.join(ordersDF, "ordersn")
			// 选取字段
    		.select(
			    $"memberid".as("uid"),
			    $"label", $"color", $"product"
		    )
		/*
		root
		 |-- uid: string (nullable = true)
		 |-- label: integer (nullable = false)
		 |-- color: integer (nullable = false)
		 |-- product: integer (nullable = false)
		 */
		// genderDF.printSchema()
		/*
			+---+-----+-----+-------+
			|uid|label|color|product|
			+---+-----+-----+-------+
			|129|0    |11   |2      |
			|129|0    |1    |8      |
			|826|1    |3    |10     |
			|933|0    |8    |19     |
		 */
//		println("===============genderDF===============")
//		genderDF.show(20, truncate = false)

		
		// 4. 获取性别信息，进行打标签
		val predictionDF: DataFrame = genderDF.select($"uid", $"label".as("prediction"))
		//predictionDF.printSchema()
//		println("===============predictionDF===============")
//		predictionDF.show(100, truncate = false)
		/*
		+---+----------+
		|uid|prediction|
		+---+----------+
		|410|0         |
		|410|0         |
		|739|0         |
		+---+----------+
		 */
		
		// 5. 计算每个用户所有购买商品中，男、女商品占比
		val usgDF: DataFrame = predictionDF
			.select(
				$"uid", //
				// 当prediction为0时，表示男性商品
				when($"prediction" === 0, 1)
					.otherwise(0).as("male"), // 男
				// 当prediction为1时，表示女性商品
				when($"prediction" === 1, 1)
					.otherwise(0).as("female") // 女
			)
			.groupBy($"uid")
			.agg(
				count($"uid").as("total"), // 用户购物次数
				sum($"male").as("maleTotal"), //预测为男性的次数
				sum($"female").as("femaleTotal") //预测为女性的次数
			)
		
		// 6. 直接获取属性标签规则rule
		val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
		// 采用广播变量广播
		val ruleMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(ruleMap)
		
		// 7. 自定义UDF函数，计算每个用户占比
		val gender_tag_udf: UserDefinedFunction = udf(
			(total: Long, maleTotal: Double, femaleTotal: Double) => {
				// 计算男、女性商品占比
				val maleRate = maleTotal / total
				val femaleRate = femaleTotal / total
				// 规则判断
				if(maleRate >= 0.6){
					ruleMapBroadcast.value("0")
				}else if(femaleRate >= 0.6){
					ruleMapBroadcast.value("1")
				}else{
					ruleMapBroadcast.value("-1")
				}
			}
		)
		
		// 8. 打标签，给每个用户打上购物性别标签ID
		val modelDF: DataFrame = usgDF.select(
			$"uid", //
			gender_tag_udf($"total", $"maleTotal", $"femaleTotal").as("tagId")
		)
		println("===============modelDF===============")
		modelDF.show(100, truncate = false)
		/*
		+---+-----+
		|uid|tagId|
		+---+-----+
		|410|379  |
		|694|379  |
		|786|379  |
		|155|379  |
		+---+-----+
		 */
		
		// 9. 返回模型标签数据
		modelDF
	}
	
	
}

object UsgModel{
	def main(args: Array[String]): Unit = {
		val tagModel = new UsgModel()
		tagModel.executeModel(378L)
	}
}