package com.wangyuxuan.spark.demo3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/1/21 10:57
 * @description 使用spark对点击流日志数据进行分析-------PV
 */
object PV {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    // 2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile("access.log")
    // 4、统计PV
    val pv: Long = dataRDD.count()
    println(s"pv:$pv")
    // 5、关闭sc
    sc.stop()
  }
}
