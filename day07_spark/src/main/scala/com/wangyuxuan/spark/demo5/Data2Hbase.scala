package com.wangyuxuan.spark.demo5


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/1/21 16:07
 * @description 通过foreachPartition算子实现把rdd结果数据写入到hbase表中
 */
object Data2Hbase {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("Data2Hbase").setMaster("local[2]")
    // 2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、读取数据
    val usersRDD: RDD[Array[String]] = sc.textFile("users.dat").map(_.split("::"))
    // 4、通过foreachPartition算子实现把usersRDD结果数据写入到hbase表中
    usersRDD.foreachPartition(iter => {
      var connection: Connection = null
      try {
        // 4.1 获取hbase数据库连接
        val configuration: Configuration = HBaseConfiguration.create()
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181")
        connection = ConnectionFactory.createConnection(configuration)
        // 4.2 对于hbase表进行操作这里需要一个Table对象
        val table: Table = connection.getTable(TableName.valueOf("person"))
        val puts = new util.ArrayList[Put]()
        // 4.3  把数据插入到hbase表中      hbase shell : put "person",'rowkey','列族：字段'，'value'
        // create 'person','f1','f2'
        iter.foreach(line => {
          // 构建一个Put对象
          val put = new Put(line(0).getBytes())
          // 构建数据
          put.addColumn("f1".getBytes(), "gerder".getBytes(), line(1).getBytes())
          put.addColumn("f1".getBytes(), "age".getBytes(), line(2).getBytes())
          put.addColumn("f2".getBytes(), "position".getBytes(), line(3).getBytes())
          put.addColumn("f2".getBytes(), "code".getBytes(), line(4).getBytes())
          puts.add(put)
        })
        // 提交这些put数据
        table.put(puts)
      } catch {
        case e: Exception => println(e.getMessage)
      } finally {
        if (connection != null) {
          connection.close()
        }
      }
    })
  }
}
