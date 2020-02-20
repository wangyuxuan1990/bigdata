package com.wangyuxuan.sparkstreaming.demo2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 10:48
 * @description HDFS数据源
 */
object WordCountForHDFSSource {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：初始化程序入口
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountForHDFSSource")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    // 步骤二：获取数据流
    /**
     * 1：这儿填的不是文件，而是目录。
     * 目录：
     * 实时的往这个目录里面放入文件。我们的程序就会对新的文件进行处理
     * 2：HDFS，用的是高可用模式（HA模式）
     * 是逻辑名字而已。
     * 要把集群的配置文件：core-site.xml hdfs-site.xml
     * 把这两个文件放到resources这个目录下。这样就能识别了。
     */
    val lines: DStream[String] = ssc.textFileStream("hdfs://hann/tmp/test")
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
