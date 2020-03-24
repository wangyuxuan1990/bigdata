package com.wangyuxuan.flink.demo14

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/23 17:53
 * @description Time Window窗口的应用
 */
object TimeWindowFirst {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val socketSource: DataStream[String] = environment.socketTextStream("node01", 9999)

    /**
     * 需求：每隔5S钟时间，统计最近10S出现的数据
     */
    socketSource.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      //      .timeWindow(Time.seconds(10)) // 接受一个参数就是一个滚动窗口
      .timeWindow(Time.seconds(10), Time.seconds(5)) // 接受两个参数，就是一个滑动窗口。如果时间值是一样的，就变成了滚动窗口
      .sum(1)
      .print()
    environment.execute()
  }
}
