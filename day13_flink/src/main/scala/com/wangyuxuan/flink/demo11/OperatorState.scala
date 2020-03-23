package com.wangyuxuan.flink.demo11

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/23 11:12
 * @description Operator State之ListState
 *              需求：实现每两条数据进行输出打印一次，不用区分数据的key
 */
object OperatorState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[(String, Int)] = env.fromCollection(List(
      ("Spark", 3),
      ("Hadoop", 5),
      ("Hadoop", 7),
      ("spark", 9)
    ))
    //    sourceStream.print() // 这种方式是每一条数据打印一次
    sourceStream.addSink(new OperateTaskState).setParallelism(1)
    env.execute()
  }
}

class OperateTaskState extends SinkFunction[(String, Int)] {
  // 声明列表，用于我们每两条数据打印一下
  private val listBuffer: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]

  // 每两条数据打印一下，需要将两条数据保存到哪里去
  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    listBuffer.+=(value)
    if (listBuffer.size == 2) {
      println(listBuffer)
      listBuffer.clear()
    }
  }
}