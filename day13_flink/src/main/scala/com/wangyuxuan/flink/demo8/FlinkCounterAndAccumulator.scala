package com.wangyuxuan.flink.demo8

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author wangyuxuan
 * @date 2020/3/20 14:34
 * @description Flink之Counter（计数器/累加器）
 *              需求：通过计数器来实现统计文件当中Exception关键字出现的次数
 */
object FlinkCounterAndAccumulator {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 统计tomcat日志当中exception关键字出现了多少次
    val sourceDataSet: DataSet[String] = environment.readTextFile("file:///D:\\数据\\catalina.out")
    sourceDataSet.map(new RichMapFunction[String, String] {
      var counter = new LongCounter

      override def open(parameters: Configuration): Unit = {
        // 注册累加器
        getRuntimeContext.addAccumulator("my-accumulator", counter)
      }

      override def map(value: String): String = {
        if (value.toLowerCase.contains("exception")) {
          counter.add(1)
        }
        value
      }
    }).setParallelism(4).writeAsText("D:\\数据\\t1")

    val job: JobExecutionResult = environment.execute()
    // 获取累加器，并打印累加器的值
    val a: Long = job.getAccumulatorResult[Long]("my-accumulator")
    println(a)
  }
}
