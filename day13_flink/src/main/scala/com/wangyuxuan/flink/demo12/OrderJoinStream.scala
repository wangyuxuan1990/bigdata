package com.wangyuxuan.flink.demo12

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
 * @author wangyuxuan
 * @date 2020/3/23 13:22
 * @description 需求：将两个流中，订单号一样的数据合并在一起输出
 *              orderinfo1数据
 *              123,大力丸,30.0
 *              234,二十六位,20.0
 *              345,海参精华,114.4
 *              333,千年老参,112.2
 *              444,黑玉续命膏,30000.0
 *              orderinfo2数据
 *              123,2019-11-11 10:11:12,东莞
 *              234,2019-11-11 11:11:13,惠州
 *              345,2019-11-11 12:11:14,江南
 *              333,2019-11-11 13:11:15,欧美
 *              444,2019-11-11 14:11:16,日韩
 */
case class OrderInfo1(orderId: Long, productName: String, price: Double)

case class OrderInfo2(orderId: Long, orderDate: String, address: String)

object OrderJoinStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 将state存储到jobManager里面的内存里面，不推荐 使用
    // environment.setStateBackend(new MemoryStateBackend())
    // 将state存放到文件系统里面去
    environment.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink_state"))
    // 将state保存到rockets-DB里面去
    // environment.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink/checkDir",true))
    // 固定间隔重启，尝试重启5次，每次间隔时间10000毫秒  一般用这种方式 实际工作当中次数一般在3-5次
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000))
    // 基于失败率的重启 用的不多
    // environment.setRestartStrategy(RestartStrategies.failureRateRestart(20, org.apache.flink.api.common.time.Time.seconds(10), org.apache.flink.api.common.time.Time.seconds(10)))
    // 不重启
    //environment.setRestartStrategy(RestartStrategies.noRestart())

    // 默认checkpoint功能是disabled的，想要使用的时候需要先启用
    // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
    environment.enableCheckpointing(1000)
    // 高级选项：
    // 设置模式为exactly-once （这是默认值）
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    environment.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
    /**
     * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
     * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
     */
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    import org.apache.flink.api.scala._
    // 读取两个自定义数据源
    val orderInfo1: DataStream[String] = environment.addSource(new FileSourceFunction("D:\\数据\\orderInfo1.txt"))
    val orderInfo2: DataStream[String] = environment.addSource(new FileSourceFunction("D:\\数据\\orderInfo2.txt"))
    // 订单一数据按照订单id进行分组
    val orderInfo1Stream: KeyedStream[OrderInfo1, Long] = orderInfo1.map(x => OrderInfo1(x.split(",")(0).toLong, x.split(",")(1), x.split(",")(2).toDouble))
      .keyBy(_.orderId)
    // 订单二数据按照订单id进行分组
    val orderInfo2Stream: KeyedStream[OrderInfo2, Long] = orderInfo2.map(x => OrderInfo2(x.split(",")(0).toLong, x.split(",")(1), x.split(",")(2)))
      .keyBy(_.orderId)
    orderInfo1Stream.connect(orderInfo2Stream).flatMap(new MyRichFlatMapFunction).print()
    environment.execute()
  }
}

class MyRichFlatMapFunction extends RichCoFlatMapFunction[OrderInfo1, OrderInfo2, (OrderInfo1, OrderInfo2)] {
  private var orderInfo1ValueState: ValueState[OrderInfo1] = _
  private var orderInfo2ValueState: ValueState[OrderInfo2] = _

  override def open(parameters: Configuration): Unit = {
    // 获取每个订单对应的存储的状态
    val order1InfoState: ValueStateDescriptor[OrderInfo1] = new ValueStateDescriptor[OrderInfo1]("order1InfoState", classOf[OrderInfo1])
    val order2InfoState: ValueStateDescriptor[OrderInfo2] = new ValueStateDescriptor[OrderInfo2]("order2InfoState", classOf[OrderInfo2])
    orderInfo1ValueState = getRuntimeContext.getState(order1InfoState)
    orderInfo2ValueState = getRuntimeContext.getState(order2InfoState)
  }

  /**
   * 针对订单一使用的方法
   *
   * @param orderInfo1
   * @param out
   */
  override def flatMap1(orderInfo1: OrderInfo1, out: Collector[(OrderInfo1, OrderInfo2)]): Unit = {
    val orderInfo2: OrderInfo2 = orderInfo2ValueState.value()
    if (orderInfo2 != null) {
      orderInfo2ValueState.clear() // 用完了之后，就清除了状态
      out.collect((orderInfo1, orderInfo2))
    } else {
      orderInfo1ValueState.update(orderInfo1)
    }
  }

  /**
   * 针对订单二使用的方法
   *
   * @param orderInfo2
   * @param out
   */
  override def flatMap2(orderInfo2: OrderInfo2, out: Collector[(OrderInfo1, OrderInfo2)]): Unit = {
    val orderInfo1: OrderInfo1 = orderInfo1ValueState.value()
    if (orderInfo1 != null) {
      orderInfo1ValueState.clear() // 用完了之后，就清除了状态
      out.collect((orderInfo1, orderInfo2))
    } else {
      orderInfo2ValueState.update(orderInfo2)
    }
  }
}

class FileSourceFunction(filePath: String) extends SourceFunction[String] {
  private val random: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    // scala操作文件
    val bufferedSource: BufferedSource = Source.fromFile(filePath, "GBK")
    val lines: Iterator[String] = bufferedSource.getLines()
    while (lines.hasNext) {
      TimeUnit.MILLISECONDS.sleep(random.nextInt(500))
      ctx.collect(lines.next())
    }
    bufferedSource.close()
  }

  override def cancel(): Unit = {

  }
}