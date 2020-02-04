package com.wangyuxuan.spark.demo7

import org.apache.spark.sql.SparkSession

/**
 * @author wangyuxuan
 * @date 2020/2/4 2:39 下午
 * @description 利用sparksql操作hivesql
 */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    // 1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()

    // 2、直接使用sparkSession去操作hivesql语句
    // 2.1 创建一张hive表
    spark.sql("create table people(id string,name string,age int) row format delimited fields terminated by ','")

    // 2.2 加载数据到hive表中
    spark.sql("load data local inpath './day07_spark/data/people.txt' into table people")

    // 2.3 查询
    spark.sql("select * from people").show()

    spark.stop()
  }
}
