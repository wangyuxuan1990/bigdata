package com.wangyuxuan.flink.demo3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/18 17:37
 * @description union
 */
object UnionStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val firstStream: DataStream[String] = environment.fromCollection(Array("spark flink", "scala study"))
    val secondStream: DataStream[String] = environment.fromCollection(Array("test now", "hive get"))
    // 两个流合并成为一个流，必须保证两个流当中的数据类型是一致的
    val resultStream: DataStream[String] = firstStream.union(secondStream)
    resultStream.print()
    environment.execute()
  }
}
