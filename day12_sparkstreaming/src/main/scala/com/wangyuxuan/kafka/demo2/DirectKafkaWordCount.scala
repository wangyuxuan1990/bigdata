package com.wangyuxuan.kafka.demo2

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/24 17:10
 * @description 基于Direct 0.8版本
 */
object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：初始化程序入口
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaWordCount")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    /**
     * 0.8 API  -> 消费  1.0集群的数据
     */
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "zookeeper.connect" -> "node01:2181,node02:2181,node03:2181",
      "group.id" -> "test2",
      "auto.commit.enable" -> "false"
    )

    //    val topics = "sparkstreaming".split(",").toSet()
    val topics: Set[String] = Set("sparkstreaming")

    // 步骤二：获取数据源
    /**
     * ssc: StreamingContext,
     * kafkaParams: Map[String, String],
     * topics: Set[String]
     */
    val lines: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    ).map(_._2)

    // 步骤三：业务代码处理
    val result: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
