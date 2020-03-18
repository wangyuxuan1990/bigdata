package com.wangyuxuan.flink.demo2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/18 15:51
 * @description 自定义单并行度数据源
 */
object MySourceRun {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 无法设置并行度，除非设置并行度是1
    val getSource: DataStream[Long] = environment.addSource(new MySource).setParallelism(1)
    val resultStream: DataStream[Long] = getSource.filter(x => x % 2 == 0)
    resultStream.setParallelism(1).print()
    environment.execute()
  }
}
