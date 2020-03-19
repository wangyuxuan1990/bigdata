package com.wangyuxuan.flink.demo6

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/19 17:17
 * @description distinct
 */
object FlinkDistinct {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val arrayBuffer: ArrayBuffer[String] = new ArrayBuffer[String]()
    arrayBuffer.+=("hello world1")
    arrayBuffer.+=("hello world2")
    arrayBuffer.+=("hello world3")
    arrayBuffer.+=("hello world4")
    println(arrayBuffer.size)
    val collectionDataSet: DataSet[String] = environment.fromCollection(arrayBuffer)
    val dsDataSet: DataSet[String] = collectionDataSet.flatMap(x => x.split(" ")).distinct()
    dsDataSet.print()
    environment.execute()
  }
}
