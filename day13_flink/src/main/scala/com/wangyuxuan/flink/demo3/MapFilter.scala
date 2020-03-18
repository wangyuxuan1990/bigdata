package com.wangyuxuan.flink.demo3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/18 17:22
 * @description map和filter
 */
object MapFilter {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[Int] = environment.fromElements(1, 2, 3, 4, 5)
    val mapStream: DataStream[Int] = sourceStream.map(x => {
      println(x)
      x
    })
    val resultStream: DataStream[Int] = mapStream.filter(x => x % 2 == 0)
    resultStream.print()
    environment.execute()
  }
}
