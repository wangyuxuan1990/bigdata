package com.wangyuxuan.spark.demo8

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangyuxuan
 * @date 2020/2/4 3:39 下午
 * @description 通过sparksql把结果数据写入到mysql表中
 */
object Data2Mysql {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("Data2Mysql").master("local[2]").getOrCreate()
    //    val spark: SparkSession = SparkSession.builder().appName("Data2Mysql").getOrCreate()
    // 2、读取mysql表中数据
    // 2.1 定义url连接
    val url = "jdbc:mysql://node03:3306/spark"
    // 2.2 定义表名
    val tableName = "person"
    // 2.3 定义属性
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)
    // 把dataFrame注册成一张表
    mysqlDF.createTempView("person")
    // 通过sparkSession调用sql方法
    val result: DataFrame = spark.sql("select * from person where age > 20")
    // 保存结果数据到mysql表中
    // mode:指定数据的插入模式
    // overwrite: 表示覆盖，如果表不存在，事先帮我们创建
    // append   :表示追加， 如果表不存在，事先帮我们创建
    // ignore   :表示忽略，如果表事先存在，就不进行任何操作
    // error    :如果表事先存在就报错（默认选项）
    result.write.mode("append").jdbc(url, "person1", properties)

    // spark-submit \
    // --master spark://node01:7077 \
    // --class com.wangyuxuan.spark.demo8.Data2Mysql \
    // --executor-memory 1g \
    // --total-executor-cores 2 \
    // --driver-class-path /bigdata/install/mysql-connector-java-5.1.38.jar \
    // --jars /bigdata/install/mysql-connector-java-5.1.38.jar \
    // original-day07_spark-1.0-SNAPSHOT.jar \
    // append person2
    //
    // --driver-class-path：指定一个Driver端所需要的额外jar
    // --jars ：指定executor端所需要的额外jar
    //    result.write.mode(args(0)).jdbc(url, args(1), properties)
    // 关闭
    spark.stop()
  }
}
