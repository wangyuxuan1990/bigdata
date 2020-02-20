package com.wangyuxuan.sparkstreaming.demo4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 16:49
 * @description window算子
 *              每隔2秒计算最近4秒的单词出现的次数
 */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDStream: DStream[String] = dataDStream.flatMap(_.split(","))
    val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    /**
     * reduceFunc: (V, V) => V,
     * windowDuration: Duration,
     * slideDuration: Duration
     */
    val result: DStream[(String, Int)] = wordAndOneDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(4), Seconds(2))
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
