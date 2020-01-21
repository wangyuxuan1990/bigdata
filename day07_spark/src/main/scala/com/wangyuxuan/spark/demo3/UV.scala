package com.wangyuxuan.spark.demo3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/1/21 11:18
 * @description 使用spark程序对点击流日志数据进行分析--------UV
 */
object UV {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    // 2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile("access.log")
    // 4、获取所有的ip地址
    val ipsRDD: RDD[String] = dataRDD.map(_.split(" ")(0))
    // 5、对ip地址进行去重
    val distinctRDD: RDD[String] = ipsRDD.distinct()
    // 6、统计uv
    val uv: Long = distinctRDD.count()
    println(s"uv:$uv")

    sc.stop()
  }
}
