package com.wangyuxuan.kafka.demo1

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/23 9:38 下午
 * @description 基于receiver 0.8版本
 *              简单知道就可以，因为这个在工作不用
 */
object ReceiverKafkaWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：初始化程序入口
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ReceiverKafkaWordCount")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams: Map[String, String] = Map[String, String](
      "zookeeper.connect" -> "node01:2181,node02:2181,node03:2181",
      "group.id" -> "test1"
    )

    //    val topics = "sparkstreaming".split(",").map((_, 1)).toMap
    val topics: Map[String, Int] = Map("sparkstreaming" -> 1)
    // 步骤二：获取数据源
    // 默认只会有一个receiver
    //    val kafkaStreams = (1 to 20).map(_ => {
    //      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    //        ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER)
    //
    //    })
    //    val lines = ssc.union(kafkaStreams)
    val kafkaStreams: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER
    )

    // 步骤三：业务代码处理
    kafkaStreams.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
