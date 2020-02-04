package com.wangyuxuan.spark.demo10

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangyuxuan
 * @date 2020/2/4 5:30 下午
 * @description 自定义sparksql的UDF函数    一对一的关系
 */
object SparkSQLFunction {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("SparkSQLFunction").master("local[2]").getOrCreate()
    // 2、构建数据源生成DataFrame
    val dataFrame: DataFrame = spark.read.text("./day07_spark/data/test_udf_data.txt")
    // 3、注册成表
    dataFrame.createTempView("t_udf")
    // 4、实现自定义的UDF函数
    // 小写转大写
    spark.udf.register("low2Up", new UDF1[String, String]() {
      override def call(t1: String): String = {
        t1.toUpperCase
      }
    }, StringType)
    // 大写转小写
    spark.udf.register("up2Low", (x: String) => x.toLowerCase())
    // 5、把数据文件中的单词统一转换成大小写
    spark.sql("select value from t_udf").show()
    spark.sql("select low2Up(value) from t_udf").show()
    spark.sql("select up2Low(value) from t_udf").show()

    spark.stop()
  }
}
