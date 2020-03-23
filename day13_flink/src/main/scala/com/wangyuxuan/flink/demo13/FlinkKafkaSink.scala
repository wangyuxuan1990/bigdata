package com.wangyuxuan.flink.demo13

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * @author wangyuxuan
 * @date 2020/3/23 17:12
 * @description 将kafka作为flink的sink来使用
 */
object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置statebackend
    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_kafka_sink/checkpoints", true))

    val socketStream: DataStream[String] = env.socketTextStream("node01", 9999)
    val topic: String = "flink"
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    prop.setProperty("group.id", "kafka_group1")
    // 第一种解决方案，设置FlinkKafkaProducer里面的事务超时时间
    // FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer<>(brokerList, topic, new SimpleStringSchema())
    // 第二种解决方案，设置kafka的最大事务超时时间
    // 设置事务超时时间
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    /**
     * 使用支持仅一次语义的形式
     */
    val kafkaSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    socketStream.addSink(kafkaSink)
    env.execute("StreamingFromCollectionScala")
  }
}
