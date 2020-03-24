package com.wangyuxuan.flink.demo14

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/24 10:49
 * @description 全量聚合统计
 */
object FlinkCountWindowAvg {
  /**
   * 输入数据
   * 1
   * 2
   * 3
   * 4
   * 5
   * 6
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val socketStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    // 统计一个窗口内的数据的平均值
    socketStream.map(x => (1, x.toInt))
      .keyBy(0)
      .countWindow(3)
      // 如果需要处理全量值，需要使用底层的函数process算子
      .process(new MyProcessWindowFunctionclass)
      .print()
    // 必须调用execute方法，否则程序不会执行
    environment.execute("count avg")
  }
}

/**
 * ProcessWindowFunction 需要跟四个参数
 * 输入参数类型，输出参数类型，聚合的key的类型，window的下界
 *
 */
class MyProcessWindowFunctionclass extends ProcessWindowFunction[(Int, Int), Double, Tuple, GlobalWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[Double]): Unit = {
    var totalNum: Int = 0
    var countNum: Int = 0
    for (data <- elements) {
      totalNum += 1
      countNum += data._2
    }
    out.collect(countNum / totalNum)
  }
}