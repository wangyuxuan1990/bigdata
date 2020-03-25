package com.wangyuxuan.flink.demo10

import java.util.Collections
import java.{lang, util}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author wangyuxuan
 * @date 2020/3/21 6:56 下午
 * @description 现在有一批数据，求取同一个用户名，三次登录时间间隔小于30S，发送报警邮件
 *              ip地址  登录用户名  登录时间
 */
object CheckLogin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      ("172.168.10.100", "zhangsan", 1584788410000L),
      ("172.168.10.100", "zhangsan", 1584788420000L),
      ("172.168.10.100", "zhangsan", 1584788430000L),
      ("172.168.10.100", "zhangsan", 1584788440000L),
      ("172.168.10.100", "zhangsan", 1584788480000L),
      ("172.168.10.100", "zhangsan", 1584788490000L),
      ("172.168.10.100", "zhangsan", 1584788500000L),
      ("172.168.10.100", "zhangsan", 1584788550000L),
      ("172.168.10.100", "zhangsan", 1584788560000L),
      ("10.12.3.10", "lisi", 1584799410000L),
      ("10.12.3.10", "lisi", 1584799450000L),
      ("10.12.3.10", "lisi", 1584799500000L),
      ("10.12.3.10", "lisi", 1584799510000L),
      ("10.12.3.10", "lisi", 1584799520000L),
      ("10.12.3.10", "lisi", 1584799530000L),
      ("10.12.3.10", "lisi", 1584799540000L)
    )).keyBy(_._2)
      .flatMap(new CountLoginWithList)
      .print()
    env.execute()
  }
}

class CountLoginWithList extends RichFlatMapFunction[(String, String, Long), String] {
  private var userLoginByKey: ListState[(String, String, Long)] = _

  override def open(parameters: Configuration): Unit = {
    val loginState: ListStateDescriptor[(String, String, Long)] = new ListStateDescriptor[(String, String, Long)]("loginState", classOf[(String, String, Long)])
    userLoginByKey = getRuntimeContext.getListState(loginState)
  }

  override def flatMap(login: (String, String, Long), out: Collector[String]): Unit = {
    val currentState: lang.Iterable[(String, String, Long)] = userLoginByKey.get()
    if (currentState == null) {
      userLoginByKey.addAll(Collections.emptyList())
    }
    userLoginByKey.add(login)
    import scala.collection.JavaConverters._
    val allLoginList: List[(String, String, Long)] = userLoginByKey.get().iterator().asScala.toList
    allLoginList.sortBy(x => x._3)
    if (allLoginList.size == 3) {
      val firstLogin: (String, String, Long) = allLoginList(0)
      val lastLogin: (String, String, Long) = allLoginList(2)
      if ((lastLogin._3 - firstLogin._3) <= 30000) {
        for (eachLogin <- allLoginList) {
          println(eachLogin.toString())
        }
      }
      val tuples: util.ArrayList[(String, String, Long)] = new util.ArrayList[(String, String, Long)]()
      for (eachLogin <- allLoginList.tail) {
        tuples.add(eachLogin)
      }
      userLoginByKey.update(tuples)
    }
  }
}