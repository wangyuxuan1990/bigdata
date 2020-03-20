package com.wangyuxuan.flink.demo6

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/20 9:08
 * @description Cross
 */
object CrossJoin {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val array1 = new ArrayBuffer[Tuple2[Int, String]]()
    val tuple1 = new Tuple2[Int, String](1, "张三")
    val tuple2 = new Tuple2[Int, String](2, "李四")
    val tuple3 = new Tuple2[Int, String](3, "王五")
    array1.+=(tuple1)
    array1.+=(tuple2)
    array1.+=(tuple3)

    val array2 = new ArrayBuffer[Tuple2[Int, String]]()
    val tuple4 = new Tuple2[Int, String](1, "18")
    val tuple5 = new Tuple2[Int, String](2, "35")
    val tuple6 = new Tuple2[Int, String](3, "42")
    array2.+=(tuple4)
    array2.+=(tuple5)
    array2.+=(tuple6)

    val firstDataSet: DataSet[(Int, String)] = environment.fromCollection(array1)
    val seccondDataSet: DataSet[(Int, String)] = environment.fromCollection(array2)

    val crossJoin: CrossDataSet[(Int, String), (Int, String)] = firstDataSet.cross(seccondDataSet)

    crossJoin.print()
    environment.execute()
  }
}
