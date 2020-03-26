package com.wangyuxuan.flink.demo17

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/26 10:20
 * @description 需求：创建订单之后15分钟之内一定要付款，否则就取消订单
 *              规则，出现1创建订单标识之后，紧接着需要在15分钟之内出现2支付订单操作，中间允许有其他操作
 */
case class OrderDetail(orderId: String, status: String, orderCreateTime: String, price: Double)

object OrderTimeOutCheckCEP {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  /**
   * 订单数据格式如下类型：
   * 字段说明：
   * 订单编号，
   * 订单状态:1.创建订单,等待支付  2.支付订单完成  3.取消订单，申请退款  4.已发货  5.确认收货，已经完成，
   * 订单创建时间，
   * 订单金额
   * 20160728001511050311389390,1,2016-07-28 00:15:11,295
   * 20160801000227050311955990,1,2016-07-28 00:16:12,165
   * 20160728001511050311389390,2,2016-07-28 00:18:11,295
   * 20160801000227050311955990,2,2016-07-28 00:18:12,165
   * 20160728001511050311389390,3,2016-07-29 08:06:11,295
   * 20160801000227050311955990,4,2016-07-29 12:21:12,165
   * 20160804114043050311618457,1,2016-07-30 00:16:15,132
   * 20160801000227050311955990,5,2016-07-30 18:13:24,165
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    val keyedStream: KeyedStream[OrderDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      OrderDetail(strings(0), strings(1), strings(2), strings(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
      override def extractTimestamp(element: OrderDetail): Long = {
        format.parse(element.orderCreateTime).getTime
      }
    }).keyBy(x => x.orderId)

    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(order => order.status.equals("1"))
      .followedBy("second")
      .where(order => order.status.equals("2"))
      .within(Time.minutes(15))

    // 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag: OutputTag[OrderDetail] = new OutputTag[OrderDetail]("orderTimeout")
    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream, pattern)
    val selectResultStream: DataStream[OrderDetail] = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutPatternFunction, new OrderPatternFunction)
    selectResultStream.print()
    // 打印输出流数据 过了15分钟还没支付的数据
    selectResultStream.getSideOutput(orderTimeoutOutputTag).print()
    environment.execute()
  }
}

class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail, OrderDetail] {
  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], timeoutTimestamp: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号为" + detail)
    detail
  }
}

class OrderPatternFunction extends PatternSelectFunction[OrderDetail, OrderDetail] {
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }
}