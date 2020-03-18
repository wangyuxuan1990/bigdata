package com.wangyuxuan.flink.demo2

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
 * @author wangyuxuan
 * @date 2020/3/18 15:44
 * @description 自定义多并行度数据源
 */
class MultipartSource extends ParallelSourceFunction[Long] {
  private var number = 1L

  private var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      number += 1
      sourceContext.collect(number)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
