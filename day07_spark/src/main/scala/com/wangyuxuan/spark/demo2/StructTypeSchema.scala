package com.wangyuxuan.spark.demo2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author wangyuxuan
 * @date 2020/1/20 17:36
 * @description 通过动态指定dataFrame对应的schema信息将rdd转换成dataFrame
 */
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    // 1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    // 2、获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    // 3、读取文件数据
    val data: RDD[Array[String]] = sc.textFile("person.txt").map(_.split(" "))
    // 4、将rdd与Row对象进行关联
    val rowRDD = data.map(x => Row(x(0), x(1), x(2).toInt))
    // 5、指定dataFrame的schema信息
    // 这里指定的字段个数和类型必须要跟Row对象保持一致
    val schema = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil
    )

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame.printSchema()
    dataFrame.show()

    dataFrame.createTempView("user")
    spark.sql("select * from user").show()

    spark.stop()
  }
}
