package com.wangyuxuan.flink.demo3

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/18 17:40
 * @description 和union类似，但是只能连接两个流，两个流的数据类型可以不同，
 *              会对两个流中的数据应用不同的处理方法
 */
object ConnectStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val firstStream: DataStream[String] = environment.fromCollection(Array("hello world", "spark flink"))
    val secondStream: DataStream[Int] = environment.fromCollection(Array(1, 2, 3, 4))
    val connectStream: ConnectedStreams[String, Int] = firstStream.connect(secondStream)
    // 使用connectStream 调用map需要传递两个函数或者传入一个CoMapFunction
    val unionStream: DataStream[Any] = connectStream.map(x => x + "abc", y => y * 2)

    val coFlatMapStream: DataStream[String] = connectStream.flatMap(new CoFlatMapFunction[String, Int, String] {
      override def flatMap1(value: String, out: Collector[String]): Unit = {
        out.collect(value.toUpperCase)
      }

      override def flatMap2(value: Int, out: Collector[String]): Unit = {
        out.collect(value * 2 + "")
      }
    })

    unionStream.print()

    coFlatMapStream.print()

    environment.execute()
  }
}
