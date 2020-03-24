package com.wangyuxuan.flink.demo15

import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/24 13:42
 * @description watermark解决无序数据问题
 *              需求：每隔5S统计最近10S单词出现的次数，数据会有延迟，使用eventtime结合watermark来实现乱序数据的正确处理
 */
object TimeWindowCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    environment.setParallelism(1)
    // 步骤一：设置时间类型
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream: DataStream[String] = environment.addSource(new TestSource)
    sourceStream.map(x => (x.split(",")(0), x.split(",")(1).toLong))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        override def getCurrentWatermark: Watermark = {
          // 每个window延迟5秒触发
          new Watermark(System.currentTimeMillis() - 5000)
        }

        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          element._2
        }
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new SumProcessFunction2)
      .print()
      .setParallelism(1)
    environment.execute()
  }
}

class SumProcessFunction2 extends ProcessWindowFunction[(String, Long), (String, Integer), Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Integer)]): Unit = {
    var sum: Int = 0
    for (eachElement <- elements) {
      sum += 1
    }
    // 输出单词出现的次数
    out.collect((key.getField(0), sum))
  }
}

import scala.util.control.Breaks._

class TestSource extends SourceFunction[String] {
  private val format: FastDateFormat = FastDateFormat.getInstance("HH:mm:ss")

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    var timeStr: String = System.currentTimeMillis().toString
    // 判断10s的整数倍发送数据
    while (timeStr.substring(timeStr.length - 4).toInt > 100) {
      breakable {
        timeStr = System.currentTimeMillis().toString
        break()
      }
    }

    println("开始发送数据时间为" + format.format(System.currentTimeMillis()))

    // 休眠  保证在13S的时候发送数据
    TimeUnit.SECONDS.sleep(13)
    ctx.collect("hadoop," + System.currentTimeMillis())

    // 产生了一个事件，但是由于网络原因，事件没有发送
    var event: String = "hadoop," + System.currentTimeMillis()

    // 第16秒发送一个事件
    TimeUnit.SECONDS.sleep(3)
    ctx.collect("hadoop," + System.currentTimeMillis())

    // 第19秒的时候发送
    TimeUnit.SECONDS.sleep(3)
    ctx.collect(event)
    TimeUnit.SECONDS.sleep(300)
  }

  override def cancel(): Unit = {

  }
}