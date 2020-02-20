package com.wangyuxuan.sparkstreaming.demo6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/2/20 2:46 下午
 * @description SparkStreaming和SparkSQL整合
 *              SparkCore:
 *                        RDD
 *              SparkSQL:
 *                        DataSet/DataFrame
 *                        rdd -> dataframe(dataset) -> table -> sql
 *              SparkStreaming:
 *                        DStream:
 *                            transform:
 *                            foreacRdd:
 *                              rdd -> dataframe(dataset) -> table -> sql
 *
 *              RDD DataFrame DataSet 进行无缝的切换
 */

object StreamAndSQLTest {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamAndSQLTest")
    val sc: SparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(","))

    // 获取到的就是一个一个的单词
    words.foreachRDD(rdd => {
      val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      // 隐式转换
      import spark.implicits._
      val wordDataFrame: DataFrame = rdd.toDF("word")
      // 注册了一个临时的视图
      wordDataFrame.createOrReplaceTempView("words")
      spark.sql("select word, count(*) as totalCount from words group by word").show()
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
