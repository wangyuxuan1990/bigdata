package com.wangyuxuan.flink.demo9

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/20 15:57
 * @description 单词计数案例再次演示
 *              注意：获取程序入口类方式变了
 */
object FlinkStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
//    environment.setParallelism(4)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)

    sourceStream.setParallelism(1)
    // 改变需求：每隔一秒钟，统计前两秒钟单词出现的次数
    val countStream: DataStream[(String, Int)] = sourceStream
      .flatMap(x => x.split(" ")).setParallelism(4) // 没有设置并行度
      .map(x => (x, 1)).setParallelism(4) // 并行度设置成为4个
      .keyBy(0) // 没法设置并行度的
//      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1).setParallelism(4) // 并行度设置成为4个
    countStream.print().setParallelism(4) // 累计求和
    environment.execute()
  }
}
