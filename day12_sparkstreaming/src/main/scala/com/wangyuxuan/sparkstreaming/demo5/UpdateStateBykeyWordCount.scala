package com.wangyuxuan.sparkstreaming.demo5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 13:50
 * @description Checkpoint
 */
object UpdateStateBykeyWordCount {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val checkpointDirectory: String = "hdfs://hann/user/wangyuxuan/checkpoint3"

    def functionToCreateContext(): StreamingContext = {
      val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateBykeyWordCount")
      val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
      ssc.checkpoint(checkpointDirectory)
      val dateDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val wordDStream: DStream[String] = dateDStream.flatMap(_.split(","))
      val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

      val result: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        // 计算出来当前的批次的单词出现的次数
        val currentCount: Int = values.sum
        // 获取上一次的结果,相当于sparkstreaming帮我们存储了上一次的状态。自动存储的。
        val lastCount: Int = state.getOrElse(0)
        // 在Scala里面，最后一行代码就是函数体的返回值
        Some(currentCount + lastCount)
      })
      result.print()
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
