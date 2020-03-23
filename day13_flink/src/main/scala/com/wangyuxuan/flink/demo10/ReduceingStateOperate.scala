package com.wangyuxuan.flink.demo10

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/23 9:47
 * @description ReducingState 用于数据聚合
 *              需求：使用ReducingState求取每个key对应的累加之后的值
 *              ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
 *              get() 获取状态值
 *              add()  更新状态值，将数据放到状态中
 *              clear() 清除状态
 */
object ReduceingStateOperate {
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
      .flatMap(new CountWithReduceingAverageStage)
      .print()
    env.execute()
  }
}

class CountWithReduceingAverageStage extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  private var reducingState: ReducingState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val reduceSum: ReducingStateDescriptor[Double] = new ReducingStateDescriptor[Double]("reduceSum", new ReduceFunction[Double] {
      /**
       *
       * @param value1 聚合之后的数值  ==> 现在有10个值  ==> 可以理解为，最大值，最小值，累加之后的值，累计相减之后的值，平均值等等
       * @param value2 传入的数值
       * @return
       * (1L, 3d)  ==> value1 = 为空  value2 = 3
       * (1L, 5d)  ==> value1 = 3    value2 = 5
       * (1L, 7d)  ==> value1 = 8    value2 = 7
       */
      override def reduce(value1: Double, value2: Double): Double = {
        value1 + value2
      }
    }, classOf[Double])
    reducingState = getRuntimeContext.getReducingState(reduceSum)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    reducingState.add(input._2)
    // 将某个key对应的所有的数据值给累加起来了
    out.collect((input._1, reducingState.get()))
  }
}