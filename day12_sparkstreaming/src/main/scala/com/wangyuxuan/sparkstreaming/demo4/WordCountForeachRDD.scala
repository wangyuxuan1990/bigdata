package com.wangyuxuan.sparkstreaming.demo4

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangyuxuan
 * @date 2020/2/19 17:22
 * @description foreachRDD算子
 */
object WordCountForeachRDD {
  def main(args: Array[String]): Unit = {
    // 设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 步骤一：初始化程序入口
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountForeachRDD")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    // 步骤二：获取数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // 步骤三：数据处理
    val words: DStream[String] = lines.flatMap(_.split(","))
    val pairs: DStream[(String, Int)] = words.map((_, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    // 将结果保存到Mysql(一) 这句代码是不能运行的
    wordCounts.foreachRDD((rdd, time) => {
      // 创建了数据库连接
      Class.forName("com.mysql.jdbc.Driver")
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
      val statement: PreparedStatement = conn.prepareStatement(s"insert into wordcount(ts, word ,count) values (?, ?, ?)")
      // statement 要从Driver通过网络发送过来
      // 序列化的事，statement不支持序列化。
      rdd.foreach(record => {
        // 遍历每一条数据，然后把数据插入数据库。
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
      })
      statement.close()
      conn.close()
    })

    ssc.start()
    ssc.stop(false)
  }
}
