package com.wangyuxuan.flink.demo14

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/23 18:04
 * @description Count Window窗口的应用
 */
object CountWindowFirst {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val socketSource: DataStream[String] = environment.socketTextStream("node01", 9999)

    /**
     * 发送数据
     * spark 1
     * spark 2
     * spark 3
     * spark 4
     * spark 5
     * hello 100
     * hello 90
     * hello 80
     * hello 70
     * hello 60
     * hello 10
     */
    socketSource.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))
      .keyBy(0)
      .countWindow(5) // 使用countWindow 配合滚动窗口，求取最近每5个数据的最大值
      .aggregate(new AggregateFunction[(String, Int), Int, Double] {
        var initAccumulator: Int = 0

        override def createAccumulator(): Int = {
          initAccumulator
        }

        override def add(value: (String, Int), accumulator: Int): Int = {
          if (accumulator >= value._2) {
            accumulator
          } else {
            value._2
          }
        }

        override def getResult(accumulator: Int): Double = {
          accumulator
        }

        override def merge(a: Int, b: Int): Int = {
          if (a >= b) {
            a
          } else {
            b
          }
        }
      })
      .print()
    environment.execute()
  }
}
