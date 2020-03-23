package com.wangyuxuan.flink.demo10

import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/23 10:14
 * @description AggregatingState 将相同key的数据进行聚合
 *              需求：将相同key的数据聚合成为一个字符串
 */
object AggregrageStateOperate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
      .flatMap(new AggregrageState)
      .print()
    env.execute()
  }
}

class AggregrageState extends RichFlatMapFunction[(Long, Double), (Long, String)] {
  private var aggregateTotal: AggregatingState[Double, String] = _

  override def open(parameters: Configuration): Unit = {
    val aggregateStateDescriptor: AggregatingStateDescriptor[Double, String, String] = new AggregatingStateDescriptor[Double, String, String]("aggregateState", new AggregateFunction[Double, String, String] {
      // 创建一个初始化的值
      override def createAccumulator(): String = {
        "Contains"
      }

      // 对数据进行累加
      override def add(value: Double, accumulator: String): String = {
        if ("Contains".equals(accumulator)) {
          accumulator + value
        } else {
          accumulator + "and" + value
        }
      }

      override def getResult(accumulator: String): String = {
        accumulator
      }

      /**
       * 累加器合并的规则
       *
       * @param a 合并之后的值
       * @param b 传入的数据值
       * @return
       */
      override def merge(a: String, b: String): String = {
        a + "and" + b
      }
    }, classOf[String])
    aggregateTotal = getRuntimeContext.getAggregatingState(aggregateStateDescriptor)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, String)]): Unit = {
    aggregateTotal.add(input._2)
    out.collect((input._1, aggregateTotal.get()))
  }
}