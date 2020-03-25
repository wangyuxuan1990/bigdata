package com.wangyuxuan.flink.demo16

import java.util
import java.util.Collections

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/25 13:34
 * @description 需求：用户上一个操作与下一个操作IP变换报警
 *              使用state来进行实现代码
 */
case class UserLogin(ip: String, username: String, operateUrl: String, time: String)

object CheckIPChange {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)
    sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLogin(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(_._1)
      .process(new LoginCheckProcessFunction)
      .print()
    environment.execute("checkIpChange")
  }
}

class LoginCheckProcessFunction extends KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)] {
  private var listState: ListState[UserLogin] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor: ListStateDescriptor[UserLogin] = new ListStateDescriptor[UserLogin]("changeIp", classOf[UserLogin])
    listState = getRuntimeContext.getListState[UserLogin](listStateDescriptor)
  }

  override def processElement(value: (String, UserLogin), ctx: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, out: Collector[(String, UserLogin)]): Unit = {
    val logins: util.ArrayList[UserLogin] = new util.ArrayList[UserLogin]()
    listState.add(value._2)
    import scala.collection.JavaConverters._
    val list: List[UserLogin] = listState.get().iterator().asScala.toList
    // 按照操作时间进行了排序
    list.sortBy(x => x.time)
    if (list.size == 2) {
      val first: UserLogin = list(0)
      val second: UserLogin = list(1)
      if (!first.ip.equals(second.ip)) {
        println("小伙子你的IP变了，赶紧回去重新登录一下")
      }
      //移除第一个IP，保留第二个IP即可
      logins.removeAll(Collections.EMPTY_LIST)
      logins.add(second)
      listState.update(logins)
    }
    out.collect(value)
  }
}