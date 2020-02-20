package com.wangyuxuan.sparkstreaming.demo1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 9:20
 * @description SparkStreaming单词统计scala版本
 */
object WordCountOnScala {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：初始化程序入口
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnScala")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    // 步骤二：获取数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // 步骤三：数据处理
    val words: DStream[String] = lines.flatMap(_.split(","))
    val pairs: DStream[(String, Int)] = words.map((_, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    // 步骤四： 数据输出
    wordCounts.print()
    // 步骤五：启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
