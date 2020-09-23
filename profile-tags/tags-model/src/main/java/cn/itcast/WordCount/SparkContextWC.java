package cn.itcast.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collector;

import static jodd.util.StringPool.SPACE;

/**
 * @Author Harry
 * @Date 2020-09-03 09:37
 * @Description
 */
public class SparkContextWC {
    public static void main(String[] args) {

        // 1. 构建spark对象
        SparkConf conf = new SparkConf()
                .setAppName("demo")
                .setMaster("local[2]");
        SparkContext spark = new SparkContext(conf);

        // 2. 读取数据
        RDD<String> inputRDD = spark.textFile("datas\\WordCount\\WordCount.txt", 2);
        // 3. 分析数据

        // 4. 开始运行和关闭资源
        spark.stop();

    }
}
