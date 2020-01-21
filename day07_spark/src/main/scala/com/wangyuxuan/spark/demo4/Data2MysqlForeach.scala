package com.wangyuxuan.spark.demo4

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/1/21 13:37
 * @description 通过foreach算子来实现把RDD的结果数据写入到mysql表中
 */
object Data2MysqlForeach {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("Data2MysqlForeach").setMaster("local[2]")
    // 2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、读取数据
    val personRDD: RDD[String] = sc.textFile("person.txt")
    // 4、切分每一行
    val personTupleRDD: RDD[(String, String, Int)] = personRDD.map(_.split(",")).map(x => (x(0), x(1), x(2).toInt))
    // 5、使用foreach算子实现把personTupleRDD结果数据写入到指定的mysql表中
    personTupleRDD.foreach(t => {
      var connection: Connection = null
      try {
        // 5.1 获取数据库连接
        connection = DriverManager.getConnection("jdbc:mysql://node03:3306/spark", "root", "123456")
        // 5.2 定义插入数据的sql语句
        val sql = "insert into person(id, name, age) values (?, ?, ?)"
        // 5.3 获取PreParedStatement
        val ps: PreparedStatement = connection.prepareStatement(sql)
        // 5.4 获取数据 给这些？赋值
        ps.setString(1, t._1)
        ps.setString(2, t._2)
        ps.setInt(3, t._3)
        // 执行sql语句
        ps.execute()
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
