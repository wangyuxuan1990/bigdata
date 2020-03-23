package com.wangyuxuan.flink.demo10

import java.util.UUID

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/23 9:23
 * @description MapState 用于将每个key对应的数据都保存成一个map集合
 *              需求：使用MapState求取每个key对应的平均值
 */
object MapStateOperate {
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
      .flatMap(new CountWithAverageMapState)
      .print()
    env.execute()
  }
}

class CountWithAverageMapState extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  private var mapState: MapState[String, Double] = _

  override def open(parameters: Configuration): Unit = {
    // 获取历史的状态值，有可能为null的情况
    val mapStateOperator: MapStateDescriptor[String, Double] = new MapStateDescriptor[String, Double]("mapStateOperator", classOf[String], classOf[Double])
    mapState = getRuntimeContext.getMapState(mapStateOperator)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    mapState.put(UUID.randomUUID().toString, input._2)
    import scala.collection.JavaConverters._
    // mapState.values() 获取所有的map集合对应的value
    val list: List[Double] = mapState.values().iterator().asScala.toList
    // 求数据的平均值 ，一旦数据的个数大于等于3个，就计算对应的平均值
    if (list.size >= 3) {
      var count: Long = 0L
      var sum: Double = 0d
      for (eachState <- list) {
        count += 1
        sum += eachState
      }
      out.collect((input._1, sum / count))
    }
  }
}