package com.wangyuxuan.flink.demo4

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/19 15:29
 * @description 自定义分区策略
 */
object FlinkCustomerPartition {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置我们的分区数，如果不设置，默认使用CPU核数作为分区个数
    environment.setParallelism(2)
    import org.apache.flink.api.scala._
    // 获取dataStream
    val sourceStream: DataStream[String] = environment.fromElements("hello world", "spark flink", "hello world", "hive hadoop")
    val rePartition: DataStream[String] = sourceStream.partitionCustom(new MyPartitioner, x => x + "")
    rePartition.map(x => {
      println("数据的key为" + x + "线程为" + Thread.currentThread().getId)
      x
    }).print()
    environment.execute()
  }
}
