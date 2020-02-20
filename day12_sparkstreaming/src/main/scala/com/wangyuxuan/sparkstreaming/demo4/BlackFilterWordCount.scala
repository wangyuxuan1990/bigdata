package com.wangyuxuan.sparkstreaming.demo4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 16:11
 * @description transform算子
 *              黑名单过滤(工作里面经常需要到到一个业务到场景)
 */
object BlackFilterWordCount {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("BlackFilterWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

    /**
     * 我们自己模拟一个黑名单
     * 正常情况下这个黑名单不应该是自己模拟的，应该是从某个存储引擎里面读取出来的（Mysql，redis）
     */
    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(List("?", "!")).map((_, true))
    val blackListBroadcast: Broadcast[Array[(String, Boolean)]] = ssc.sparkContext.broadcast(blackListRDD.collect())

    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDStream: DStream[String] = dataDStream.flatMap(_.split(","))
    val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    /**
     * SparkCore：
     *    map:
     *        record 一条数据一条数据遍历
     *    mapPartition:
     *        Partition 一个分区一个分区遍历
     * transform(类似于 foreachRDD)
     *    RDD： 一个RDD一个RDD遍历
     */
    val filterDStream: DStream[(String, Int)] = wordAndOneDStream.transform(rdd => {
      // 接下来我们就可以用RDD编程来实现我们想要的效果
      // 这个里面使用到了外部的变量
      val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blackListBroadcast.value)
      /**
       * hello,1   left join     ?,true
       * ?,1                     !,true
       *
       * [hello,[1,None]] 要
       *
       * [?,[1,true]] 不要
       */
      val filterRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blackListRDD)
      // 剩下到数据
      val result: RDD[(String, (Int, Option[Boolean]))] = filterRDD.filter(tuple => {
        tuple._2._2.isEmpty
      })
      val wordAndOne: RDD[(String, Int)] = result.map(tuple => {
        (tuple._1, tuple._2._1)
      })
      wordAndOne
    })
    filterDStream.reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
