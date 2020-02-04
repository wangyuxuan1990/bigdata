package com.wangyuxuan.spark.demo9

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangyuxuan
 * @date 2020/2/4 4:53 下午
 * @description sparksql可以把结果数据保存到不同的外部存储介质中
 */
object SaveResult {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")
    // 2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 3、加载数据源
    val jsonDF: DataFrame = spark.read.json("./day07_spark/data/score.json")
    // 4、把DataFrame注册成表
    jsonDF.createTempView("t_score")
    // 5、统计分析
    val result: DataFrame = spark.sql("select * from t_score where score > 80")
    //保存结果数据到不同的外部存储介质中
    // 5.1 保存结果数据到文本文件  ----  保存数据成文本文件目前只支持单个字段，不支持多个字段
    result.select("name").write.text("./day07_spark/data/txt")
    // 5.2 保存结果数据到json文件
    result.write.json("./day07_spark/data/json")
    // 5.3 保存结果数据到parquet文件
    result.write.parquet("./day07_spark/data/parquet")
    // 5.4 save方法保存结果数据，默认的数据格式就是parquet
    result.write.save("./day07_spark/data/save")
    // 5.5 保存结果数据到csv文件
    result.write.csv("./day07_spark/data/csv")
    // 5.6 保存结果数据到表中
    result.write.saveAsTable("t1")
    // 5.7  按照单个字段进行分区 分目录进行存储
    result.write.partitionBy("classNum").json("./day07_spark/data/partitions")
    // 5.8  按照多个字段进行分区 分目录进行存储
    result.write.partitionBy("classNum", "name").json("./day07_spark/data/numPartitions")

    spark.stop()
  }
}
