package com.wangyuxuan.flink.demo10

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/20 17:34
 * @description 需求：使用valueState实现平均值求取
 */
object ValueStateOperate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d)
    )).keyBy(_._1) // 进行数据的分区
      .flatMap(new CountWindowAverage)
      .print()
    env.execute()
  }
}

class CountWindowAverage extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  private var sum: ValueState[(Long, Double)] = _

  // 在open初始化的方法里面获取历史保存的state
  override def open(parameters: Configuration): Unit = {
    val averageCount: ValueStateDescriptor[(Long, Double)] = new ValueStateDescriptor[(Long, Double)]("average", classOf[(Long, Double)])
    // 获取历史状态值，第一次的时候，没有历史状态值，需要判断为空的情况
    sum = getRuntimeContext.getState(averageCount)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    val tmpCurrentSum: (Long, Double) = sum.value() // 获取历史保存值  有可能为null
    val currentSum: (Long, Double) = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0d)
    }
    val newSum: (Long, Double) = (currentSum._1 + 1, currentSum._2 + input._2)
    sum.update(newSum) // 更新历史值
    // 求取平均值
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      // 将状态清除
//      sum.clear()
    }
  }
}