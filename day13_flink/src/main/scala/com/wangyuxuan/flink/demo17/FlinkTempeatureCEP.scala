package com.wangyuxuan.flink.demo17

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author wangyuxuan
 * @date 2020/3/26 9:20
 * @description 需求：现在工厂当中有大量的传感设备，用于检测机器当中的各种指标数据，
 *              例如温度，湿度，气压等，并实时上报数据到数据中心，现在需要检测，某一个传感器上报的温度数据是否发生异常。
 *              异常的定义：三分钟时间内，出现三次及以上的温度高于40度就算作是异常温度，进行报警输出。
 */
// 定义温度信息pojo
case class DeviceDetail(sensorMac: String, deviceMac: String, temperature: String, dampness: String, pressure: String, date: String)

case class AlarmDevice(sensorMac: String, deviceMac: String, temperature: String)

object FlinkTempeatureCEP {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  /**
   * 收集数据如下：
   * 传感器设备mac地址，检测机器mac地址，温度，湿度，气压，数据产生时间
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,38,0.52,1.1,2020-03-02 12:20:32
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,47,0.48,1.1,2020-03-02 12:20:35
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,50,0.48,1.1,2020-03-02 12:20:38
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,31,0.48,1.1,2020-03-02 12:20:39
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,52,0.48,1.1,2020-03-02 12:20:41
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,53,0.48,1.1,2020-03-02 12:20:43
   * 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,55,0.48,1.1,2020-03-02 12:20:45
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    val deviceStream: KeyedStream[DeviceDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
    }).assignAscendingTimestamps(x => {
      format.parse(x.date).getTime
    }).keyBy(x => x.sensorMac)

    val pattern: Pattern[DeviceDetail, DeviceDetail] = Pattern.begin[DeviceDetail]("start")
      .where(x => x.temperature.toInt >= 40)
      .followedByAny("follow")
      .where(x => x.temperature.toInt >= 40)
      .followedByAny("third")
      .where(x => x.temperature.toInt >= 40)
      .within(Time.minutes(3))

    val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream, pattern)
    patternResult.select(new MyPatternResultFunction).print()
    environment.execute("startTempeature")
  }
}

class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail, AlarmDevice] {
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {
    val startDetails: util.List[DeviceDetail] = pattern.get("start")
    val followDetails: util.List[DeviceDetail] = pattern.get("follow")
    val thirdDetails: util.List[DeviceDetail] = pattern.get("third")

    val startResult: DeviceDetail = startDetails.iterator().next()
    val followResult: DeviceDetail = followDetails.iterator().next()
    val thirdResult: DeviceDetail = thirdDetails.iterator().next()

    println("第一条数据" + startResult)
    println("第二条数据" + followResult)
    println("第三条数据" + thirdResult)
    AlarmDevice(thirdResult.sensorMac, thirdResult.deviceMac, thirdResult.temperature)
  }
}