package com.wangyuxuan.flink.demo16

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author wangyuxuan
 * @date 2020/3/25 15:06
 * @description 使用CEP方式来实现
 */
object LoginCheckWithCEP {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    // 获取到了我们的流
    val result: KeyedStream[(String, UserLogin), String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLogin(strings(0), strings(1), strings(2), strings(3)))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, UserLogin)](Time.seconds(5)) {
      // 使用BoundedOutOfOrdernessTimestampExtractor指定时间值为5s，表示允许数据最大的乱序时间为5s
      override def extractTimestamp(element: (String, UserLogin)): Long = {
        val time: Long = format.parse(element._2.time).getTime
        time
      }
    }).keyBy(_._1)

    // 判断连续的ip，更换了就进行报警 判断第一次IP与第二次IP不一样
    val pattern: Pattern[(String, UserLogin), (String, UserLogin)] = Pattern.begin[(String, UserLogin)]("begin")
      .where(x => x._2.username != null)
      .next("second")
      .where(new IterativeCondition[(String, UserLogin)] {
        /**
         *
         * @param value 传入的数据
         * @param ctx   上下文对象
         * @return
         */
        override def filter(value: (String, UserLogin), ctx: IterativeCondition.Context[(String, UserLogin)]): Boolean = {
          var flag: Boolean = false
          val firstValues: util.Iterator[(String, UserLogin)] = ctx.getEventsForPattern("begin").iterator()
          while (firstValues.hasNext) {
            val tuple: (String, UserLogin) = firstValues.next()
            if (!tuple._2.ip.equals(value._2.ip)) {
              flag = true
            }
          }
          flag
        }
      }).within(Time.seconds(120))

    // pattern与流都有了，将pattern作用到流上面去
    val patternStream: PatternStream[(String, UserLogin)] = CEP.pattern(result, pattern)
    patternStream.select(new CEPatternFunction).print()
    environment.execute()
  }
}

class CEPatternFunction extends PatternSelectFunction[(String, UserLogin), (String, UserLogin)] {
  override def select(map: util.Map[String, util.List[(String, UserLogin)]]): (String, UserLogin) = {
    val iter: util.Iterator[(String, UserLogin)] = map.get("begin").iterator()
    val tuple: (String, UserLogin) = map.get("second").iterator().next()
    val scalaIterable: Iterable[util.List[(String, UserLogin)]] = map.values().asScala
    for (eachIterable <- scalaIterable) {
      if (eachIterable.size() > 0) {
        val scalaListBuffer: mutable.Buffer[(String, UserLogin)] = eachIterable.asScala
        for (eachTuple <- scalaListBuffer) {
          //          println(eachTuple._2.operateUrl)
        }
      }
    }
    tuple
  }
}