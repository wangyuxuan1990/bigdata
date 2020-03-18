package com.wangyuxuan.flink.demo3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/18 17:32
 * @description 使用滑动窗口
 *              每隔1秒钟统计最近2秒钟的每个单词出现的次数
 */
object FlinkStream {
  def main(args: Array[String]): Unit = {
    // 获取程序入口类
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 从socket当中获取数据
    val resultDataStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    // 导入隐式转换的包
    import org.apache.flink.api.scala._
    // 对数据进行计算操作
    val resultData: DataStream[(String, Int)] = resultDataStream
      .flatMap(x => x.split(" ")) // 按照空格进行切分
      .map(x => (x, 1)) // 程序出现一次记做1
      .keyBy(0) // 按照下标为0的单词进行统计
      .timeWindow(Time.seconds(2), Time.seconds(1)) // 每隔一秒钟计算一次前两秒钟的单词出现的次数
      .sum(1)
    resultData.print()
    // 执行程序
    environment.execute()
  }
}
