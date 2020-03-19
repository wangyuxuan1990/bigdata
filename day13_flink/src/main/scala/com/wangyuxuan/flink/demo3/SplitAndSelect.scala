package com.wangyuxuan.flink.demo3

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @author wangyuxuan
 * @date 2020/3/19 14:11
 * @description 根据规则把一个数据流切分为多个流
 *              应用场景：
 *              可能在实际工作中，源数据流中混合了多种类型的数据，多种类型的数据处理规则不一样，所以就可以再根据一定的规则，
 *              把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了
 */
object SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val firstStream: DataStream[String] = environment.fromCollection(Array("hello world", "spark flink"))
    val selectStream: SplitStream[String] = firstStream.split(new OutputSelector[String] {
      override def select(value: String): lang.Iterable[String] = {
        val list: util.ArrayList[String] = new util.ArrayList[String]()
        // 如果包含hello字符串
        if (value.contains("hello")) {
          // 存放到一个叫做hello的stream里面去
          list.add("hello")
        } else {
          // 否则存放到一个叫做other的stream里面去
          list.add("other")
        }
        list
      }
    })
    // 获取hello这个stream
    selectStream.select("hello").print()
    environment.execute()
  }
}
