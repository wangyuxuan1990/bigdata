package com.wangyuxuan.flink.demo6

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/19 17:37
 * @description OutJoin
 */
object FlinkOuterJoin {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    environment.setParallelism(1)
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
    val tuple6 = new Tuple2[Int, String](4, "42")
    array2.+=(tuple4)
    array2.+=(tuple5)
    array2.+=(tuple6)

    val firstDataSet: DataSet[(Int, String)] = environment.fromCollection(array1)
    val secondDataSet: DataSet[(Int, String)] = environment.fromCollection(array2)

    // 左外连接
    val leftJoinValue: DataSet[(Int, String, String)] = firstDataSet.leftOuterJoin(secondDataSet)
      .where(0).equalTo(0).apply(new JoinFunction[(Int, String), (Int, String), Tuple3[Int, String, String]] {
      override def join(first: (Int, String), second: (Int, String)): (Int, String, String) = {
        val result: (Int, String, String) = if (second == null) {
          Tuple3[Int, String, String](first._1, first._2, "null")
        } else {
          Tuple3[Int, String, String](first._1, first._2, second._2)
        }
        result
      }
    })
    leftJoinValue.print()
    // 右外连接
    val rightJoinValue: DataSet[(Int, String, String)] = firstDataSet.rightOuterJoin(secondDataSet)
      .where(0).equalTo(0).apply(new JoinFunction[(Int, String), (Int, String), Tuple3[Int, String, String]] {
      override def join(first: (Int, String), second: (Int, String)): (Int, String, String) = {
        val result: (Int, String, String) = if (first == null) {
          Tuple3[Int, String, String](second._1, second._2, "null")
        } else {
          Tuple3[Int, String, String](second._1, second._2, first._2)
        }
        result
      }
    })
    rightJoinValue.print()
    // 满外连接
    val fullJoinValue: DataSet[(Int, String, String)] = firstDataSet.fullOuterJoin(secondDataSet)
      .where(0).equalTo(0).apply(new JoinFunction[(Int, String), (Int, String), Tuple3[Int, String, String]] {
      override def join(first: (Int, String), second: (Int, String)): (Int, String, String) = {
        val result: (Int, String, String) = if (second == null) {
          Tuple3[Int, String, String](first._1, first._2, "null")
        } else if (first == null) {
          Tuple3[Int, String, String](second._1, second._2, "null")
        } else {
          Tuple3[Int, String, String](first._1, first._2, second._2)
        }
        result
      }
    })
    fullJoinValue.print()

    environment.execute()
  }
}
