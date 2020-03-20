package com.wangyuxuan.flink.demo8

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/20 14:04
 * @description Flink之广播变量
 */
object FlinkBroadCast {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val broadData = new ArrayBuffer[Tuple2[String, Int]]()
    broadData.+=(new Tuple2("zs", 18))
    broadData.+=(new Tuple2("ls", 20))
    broadData.+=(new Tuple2("ww", 17))
    val broadCastSet: DataSet[(String, Int)] = environment.fromCollection(broadData)

    // 需要广播数据 对这个dataSet进行广播
    val mapDataSet: DataSet[(String, Int)] = broadCastSet.map(eachLine => {
      (eachLine._1, eachLine._2)
    })

    // 原始数据
    val data: DataSet[String] = environment.fromElements("zs", "ls", "ww")

    val broadCastResult: DataSet[String] = data.map(new RichMapFunction[String, String] {
      // 广播的list集合,用于下方open方法赋值
      private var tupleBroadCast: util.List[(String, Int)] = null // java的集合
      // hashMap集合
      private var allMap = new mutable.HashMap[String, Int]()

      // 初始化方法，用于获取广播变量，然后将广播变量放到一个map集合当中
      override def open(parameters: Configuration): Unit = {
        // 获取广播变量，将我们的数据，放到一个map集合里面去了
        // getRuntimeContext 上下文对象
        tupleBroadCast = getRuntimeContext.getBroadcastVariable[(String, Int)]("broadCastName")
        import scala.collection.JavaConverters._
        // 注意，一定要将java的list转换成为scala的集合，不然会报错
        val iterator: Iterator[(String, Int)] = tupleBroadCast.asScala.iterator
        while (iterator.hasNext) {
          val tuple: (String, Int) = iterator.next()
          allMap.put(tuple._1, tuple._2)
        }
      }

      override def map(in: String): String = {
        val age: Int = allMap.getOrElse(in, 20)
        in + "\t" + age
      }
    }).withBroadcastSet(mapDataSet, "broadCastName") // 对我们的数据做广播操作  broadCastName 广播出去的变量的名字

    broadCastResult.print()
    environment.execute()
  }
}
