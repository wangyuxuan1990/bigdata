package com.wangyuxuan.flink.demo2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/18 16:08
 * @description 自定义多并行度数据源
 */
object MyMultipartSourceRun {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val getSource: DataStream[Long] = environment.addSource(new MultipartSource).setParallelism(2)
    val resultStream: DataStream[Long] = getSource.filter(x => x % 2 == 0)
    resultStream.setParallelism(2).print()
    environment.execute()
  }
}
