package com.wangyuxuan.flink.demo4

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * @author wangyuxuan
 * @date 2020/3/19 14:37
 * @description 对filter之后的数据进行重新分区
 */
object FlinkPartition {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[String] = environment.fromElements("hello world", "test spark", "abc hello", "hello flink")
    val resultStream: DataStream[(String, Int)] = dataStream.filter(x => x.contains("hello"))
      // .shuffle  // 随机的重新分发数据,上游的数据，随机的发送到下游的分区里面去
      // .rescale  // 类似于做缩减分区的操作
      .rebalance // 对数据重新进行分区，涉及到shuffle的过程
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
    resultStream.print().setParallelism(1)
    environment.execute()
  }
}
