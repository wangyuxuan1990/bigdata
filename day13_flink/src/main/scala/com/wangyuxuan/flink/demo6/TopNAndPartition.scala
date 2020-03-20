package com.wangyuxuan.flink.demo6

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/20 9:40
 * @description First-n 和 SortPartition
 */
object TopNAndPartition {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val array1 = new ArrayBuffer[Tuple2[Int, String]]()
    val tuple1 = new Tuple2[Int, String](1, "张三")
    val tuple2 = new Tuple2[Int, String](2, "李四")
    val tuple3 = new Tuple2[Int, String](3, "王五")
    val tuple4 = new Tuple2[Int, String](3, "赵6")
    array1.+=(tuple1)
    array1.+=(tuple2)
    array1.+=(tuple3)
    array1.+=(tuple4)

    val collectionDataSet: DataSet[(Int, String)] = environment.fromCollection(array1)

    collectionDataSet.first(3).print()

    collectionDataSet
      .groupBy(0) // 按照第一个字段进行分组 按照指定的字段进行分组
      .sortGroup(1, Order.DESCENDING) // 按照第二个字段进行排序 对每组里面的数据进行排序
      .first(1) // 获取每组的前一个元素
      .print()

    /**
     * 不分组排序，针对所有元素进行排序，第一个元素降序，第二个元素升序
     */
    collectionDataSet.sortPartition(0, Order.DESCENDING).sortPartition(1, Order.ASCENDING).print()

    environment.execute()
  }
}
