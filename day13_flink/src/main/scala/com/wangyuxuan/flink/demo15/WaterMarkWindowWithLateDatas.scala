package com.wangyuxuan.flink.demo15

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/24 8:34 下午
 * @description 收集迟到的数据
 */
object WaterMarkWindowWithLateDatas {
  def main(args: Array[String]): Unit = {
    // 获取程序入口类
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换的包
    import org.apache.flink.api.scala._
    val outputTag: OutputTag[(String, Long)] = new OutputTag[(String, Long)]("lateDatas")
    environment.setParallelism(1)
    // 步骤一：设置时间类型
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置waterMark产生的周期为1s
    environment.getConfig.setAutoWatermarkInterval(1000)
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    val result: DataStream[String] = sourceStream.map(x => (x.split(",")(0), x.split(",")(1).toLong))
      .assignTimestampsAndWatermarks(new MyOwnWaterMark2)
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      //      .allowedLateness(Time.seconds(2)) // 允许事件迟到 2 秒
      .sideOutputLateData(outputTag)
      .process(new MySumFunction2)
    result.print().setParallelism(1)
    // 获取迟到太多的数据
    result.getSideOutput(outputTag).map(x => {
      println("迟到的数据为" + x._1 + "\t" + x._2)
      x._1 + "\t" + x._2
    }).print()
    environment.execute()
  }
}

class MySumFunction2 extends ProcessWindowFunction[(String, Long), String, Tuple, TimeWindow] {
  private val dateFormat: FastDateFormat = FastDateFormat.getInstance("HH:mm:ss")

  /**
   * 当一个window触发计算的时候会调用这个方法
   *
   * @param key      key
   * @param context  operator的上下文
   * @param elements 指定window的所有元素
   * @param out      用户输出
   */
  override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    println("程序处理时间为" + dateFormat.format(context.currentProcessingTime))
    println("window 开始时间为" + dateFormat.format(context.window.getStart))

    val strings: ListBuffer[String] = new ListBuffer[String]
    for (eachElement <- elements) {
      strings.+=(eachElement.toString() + "|" + dateFormat.format(eachElement._2))
    }
    out.collect(strings.toString())

    println("window结束时间为" + dateFormat.format(context.window.getEnd))
  }
}

class MyOwnWaterMark2 extends AssignerWithPeriodicWatermarks[(String, Long)] {
  private val format: FastDateFormat = FastDateFormat.getInstance("HH:mm:ss")
  private var currentMaxEventTime: Long = 0L
  private val maxOutOfOrderness: Long = 10000L // 最大允许的乱序时间 10 秒

  /**
   * 动态的获取我们的waterMark的值
   * 之前是当前时间，往前推动5S
   *
   * @return
   */
  override def getCurrentWatermark: Watermark = {
    // 每次都取到了最大的eventTime，允许最大的乱序时间是10S
    new Watermark(currentMaxEventTime - maxOutOfOrderness)
  }

  // 通过比较，抽取了我们最大值
  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    val currentElementEventTime: Long = element._2
    currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime)
    var log: String = "event数据为： " + element + "|" + "数据event_time为：" + format.format(element._2) + "|" + "当前数据最大event_time为：" + format.format(currentMaxEventTime) + "|" + "当前watermark值为：" + format.format(getCurrentWatermark().getTimestamp())
    println(log)
    // 数据一直在乱序的产生，需要我们每次获取最大的waterMark的值
    currentElementEventTime
  }
}
