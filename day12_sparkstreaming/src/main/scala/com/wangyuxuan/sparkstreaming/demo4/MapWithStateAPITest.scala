package com.wangyuxuan.sparkstreaming.demo4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 14:50
 * @description mapWithState算子 性能更好
 */
object MapWithStateAPITest {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("MapWithStateAPITest")
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("hdfs://hann/user/wangyuxuan/checkpoint2")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(","))
    val pairs: DStream[(String, Int)] = words.map((_, 1))

    val initialRDD: RDD[(String, Long)] = sc.parallelize(List(("hadoop", 100L), ("spark", 30L)))

    /**
     * currentBatchTime : 表示当前的Batch的时间
     * key: 表示需要更新状态的key
     * value: 表示当前batch的对应的key的对应的出现的次数
     * currentState: 对应key的上次的状态
     */
    val stateSpec: StateSpec[String, Int, Long, (String, Long)] = StateSpec.function((currentBatchTime: Time, key: String, value: Option[Int], currentState: State[Long]) => {
      val sum: Long = value.getOrElse(0).toLong + currentState.getOption().getOrElse(0L)
      // 如果你的数据没有超时
      if (!currentState.isTimingOut()) {
        // 更新状态
        currentState.update(sum)
      }
      // 要求有个返回值
      Some((key, sum))
    }).initialState(initialRDD).timeout(Seconds(10)) // 超时用到的可能性不大
    // timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉
    val result: MapWithStateDStream[String, Int, Long, (String, Long)] = pairs.mapWithState(stateSpec)
    // 只打印当前 更新的key的结果
//    result.print()
    // 会打印所有key的结果
    result.stateSnapshots().print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
