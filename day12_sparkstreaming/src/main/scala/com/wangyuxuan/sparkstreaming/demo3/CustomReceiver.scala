package com.wangyuxuan.sparkstreaming.demo3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 11:07
 * @description 自定义数据源
 */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomReceiver")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

    // 模拟socket数据源写的，实现了一个类似socket数据源的一个效果。
    val lines: ReceiverInputDStream[String] = ssc.receiverStream(new MyCustomReceiver("localhost", 9999))

    val wordCounts: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
