package com.wangyuxuan.flink.demo13

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author wangyuxuan
 * @date 2020/3/23 16:50
 * @description 将kafka作为flink的source来使用
 */
object FlinkKafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // checkpoint配置
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置statebackend
    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_kafka_sink/checkpoints", true))

    val topic: String = "flink"
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    prop.setProperty("group.id", "con1")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, prop)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val kafkaSource: DataStream[String] = env.addSource(kafkaConsumer)
    kafkaSource.print()
    env.execute()
  }
}
