package com.wangyuxuan.flink.demo6

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/20 9:59
 * @description partition
 */
object FlinkPartition {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val array1 = new ArrayBuffer[Tuple2[Int, String]]()
    array1.+=(new Tuple2(1, "hello1"))
    array1.+=(new Tuple2(2, "hello2"))
    array1.+=(new Tuple2(2, "hello3"))
    array1.+=(new Tuple2(3, "hello4"))
    array1.+=(new Tuple2(3, "hello5"))
    array1.+=(new Tuple2(3, "hello6"))
    array1.+=(new Tuple2(4, "hello7"))
    array1.+=(new Tuple2(4, "hello8"))
    array1.+=(new Tuple2(4, "hello9"))
    array1.+=(new Tuple2(4, "hello10"))
    array1.+=(new Tuple2(5, "hello11"))
    array1.+=(new Tuple2(5, "hello12"))
    array1.+=(new Tuple2(5, "hello13"))
    array1.+=(new Tuple2(5, "hello14"))
    array1.+=(new Tuple2(5, "hello15"))
    array1.+=(new Tuple2(6, "hello16"))
    array1.+=(new Tuple2(6, "hello17"))
    array1.+=(new Tuple2(6, "hello18"))
    array1.+=(new Tuple2(6, "hello19"))
    array1.+=(new Tuple2(6, "hello20"))
    array1.+=(new Tuple2(6, "hello21"))

    environment.setParallelism(2)

    val sourceDataSet: DataSet[(Int, String)] = environment.fromCollection(array1)

//    val hashPartition: DataSet[(Int, String)] = sourceDataSet.partitionByHash(0).mapPartition(eachPartition => {
//      val iterator: Iterator[(Int, String)] = eachPartition.toIterator
//      while (iterator.hasNext) {
//        val tuple: (Int, String) = iterator.next()
//        println("当前线程ID为" + Thread.currentThread().getId + "=============" + tuple._1)
//      }
//      iterator
//    })
//    hashPartition.print()

    /**
     * partitionByRange按照范围值进行分区，有可能造成数据倾斜的问题
     */
    sourceDataSet.partitionByRange(x => x._1).mapPartition(eachPartition => {
      val iterator: Iterator[(Int, String)] = eachPartition.toIterator
      while (iterator.hasNext) {
        val tuple: (Int, String) = iterator.next()
        println("当前线程ID为" + Thread.currentThread().getId + "=============" + tuple._1)
      }
      iterator
    }).print()

    environment.execute()
  }
}
