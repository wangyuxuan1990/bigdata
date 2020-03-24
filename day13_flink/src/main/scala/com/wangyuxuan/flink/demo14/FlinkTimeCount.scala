package com.wangyuxuan.flink.demo14

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/24 9:54
 * @description 增量聚合统计
 */
object FlinkTimeCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val socketStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    /**
     * 输入数据
     * 1
     * 2
     * 3
     * 4
     * 5
     * 6
     */
    val print: DataStreamSink[(Int, Int)] = socketStream.map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(Int, Int)] {
        // t为以前的聚合结果，t1为下一个数据
        override def reduce(t: (Int, Int), t1: (Int, Int)): (Int, Int) = {
          println((t._1, t._2 + t1._2))
          (t._1, t._2 + t1._2)
        }
      })
      .print()
    environment.execute("startRunning")
  }
}
