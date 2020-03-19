package com.wangyuxuan.flink.demo4

import org.apache.flink.api.common.functions.Partitioner

/**
 * @author wangyuxuan
 * @date 2020/3/19 15:24
 * @description 定义分区类
 */
class MyPartitioner extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    println("分区个数为" + numPartitions)
    if (key.contains("hello")) {
      0
    } else {
      1
    }
  }
}
