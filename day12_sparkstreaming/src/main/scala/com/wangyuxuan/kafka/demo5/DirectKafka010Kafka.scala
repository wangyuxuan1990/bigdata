package com.wangyuxuan.kafka.demo5

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/3/5 10:47
 * @description 与Kafka整合数据不丢失方案 0.10版本
 */
object DirectKafka010Kafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：获取配置信息
    val conf: SparkConf = new SparkConf().setAppName("DirectKafka010Kafka").setMaster("local[5]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val brokers: String = "node01:9092,node02:9092,node03:9092"
    val topics: String = "sparkstreaming"
    val groupId: String = "test6"
    val topicSet: Set[String] = topics.split(",").toSet

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 步骤二：获取数据源
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
    // 设置监听器
    ssc.addStreamingListener(new KafkaListener(stream))

    val result: DStream[(String, Int)] = stream.map(_.value()).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
