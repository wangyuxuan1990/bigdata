package com.wangyuxuan.flink.demo1

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author wangyuxuan
 * @date 2020/3/18 11:43
 * @description 离线代码开发
 *              对文件进行单词计数，统计文件当中每个单词出现的次数
 */
object FlinkFileCount {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    // 导入隐式转换的包
    import org.apache.flink.api.scala._
    val readFileDataSet: DataSet[String] = environment.readTextFile("file:///D:\\数据\\input", "GBK")
    val value: AggregateDataSet[(String, Int)] = readFileDataSet.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1)
    value.writeAsText("file:///D:\\数据\\output")
    environment.execute()
  }
}
