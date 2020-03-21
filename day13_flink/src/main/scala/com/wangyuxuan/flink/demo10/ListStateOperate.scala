package com.wangyuxuan.flink.demo10

import java.lang
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/21 6:06 下午
 * @description ListState 用于保存每个key的历史数据为一个列表
 *              需求：使用ListState求取数据平均值
 *              ListState<T> ：这个状态为每一个 key 保存集合的值
 *              get() 获取状态值
 *              add() / addAll() 更新状态值，将数据放到状态中
 *              clear() 清除状态
 */
object ListStateOperate {
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
      .flatMap(new CountWindowAverageWithList)
      .print()
    env.execute()
  }
}

class CountWindowAverageWithList extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  // 定义我们历史所有的数据获取 有可能为null
  private var elementsByKey: ListState[(Long, Double)] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化获取历史状态的值，每个key对应的所有历史值，都存储在list集合里面了
    val listState: ListStateDescriptor[(Long, Double)] = new ListStateDescriptor[(Long, Double)]("listState", classOf[(Long, Double)])
    elementsByKey = getRuntimeContext.getListState(listState)
  }

  override def flatMap(element: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    val currentState: lang.Iterable[(Long, Double)] = elementsByKey.get() // 获取当前key的状态值
    // 如果初始状态为空，那么就进行初始化，构造一个空的集合出来，准备用于存储后续的数据
    if (currentState == null) {
      elementsByKey.addAll(Collections.emptyList())
    }
    elementsByKey.add(element)
    import scala.collection.JavaConverters._
    val allElements: Iterator[(Long, Double)] = elementsByKey.get().iterator().asScala
    val allElementList: List[(Long, Double)] = allElements.toList // 获取到历史列表
    // 判断数据大于3个，就进行计算平均值
    if (allElementList.size >= 3) {
      var count: Long = 0L
      var sum: Double = 0d
      for (eachElement <- allElementList) {
        count += 1
        sum += eachElement._2
      }
      out.collect((element._1, sum / count))
    }
  }
}