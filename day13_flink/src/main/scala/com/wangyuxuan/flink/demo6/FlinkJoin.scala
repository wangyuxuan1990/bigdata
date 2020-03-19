package com.wangyuxuan.flink.demo6

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/19 17:20
 * @description join
 */
object FlinkJoin {
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
    val secondDataSet: DataSet[(Int, String)] = environment.fromCollection(array2)

    val joinResult: UnfinishedJoinOperation[(Int, String), (Int, String)] = firstDataSet.join(secondDataSet)
    // 第一个流join第二个流，需要指定join的条件
    val resultDataSet: DataSet[(Int, String, String)] = joinResult.where(0).equalTo(0).map(x => {
      (x._1._1, x._1._2, x._2._2)
    })

    resultDataSet.print()
    environment.execute()
  }
}
