package com.wangyuxuan.flink.demo2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/18 13:44
 * @description 数据源之collection
 */
object StreamingSourceFromCollection {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换的包
    import org.apache.flink.api.scala._
    val arr = Array("hello world", "world spark", "flink test", "spark hive", "test")
    val fromArray: DataStream[String] = environment.fromCollection(arr)
    //  val value: DataStream[String] = environment.fromElements("hello world")
    val resultDataStream: DataStream[(String, Int)] = fromArray.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
    resultDataStream.setParallelism(1).print()
    environment.execute()
  }
}
