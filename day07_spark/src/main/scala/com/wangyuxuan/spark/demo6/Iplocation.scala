package com.wangyuxuan.spark.demo6

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/1/21 17:11
 * @description 通过spark来实现ip的归属地查询
 */
object Iplocation {
  // 把ip地址转换成Long类型数字
  def ip2Long(ip: String): Long = {
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  // 利用二分查询，查询到数字在数组中的下标
  def binarySearch(ipNum: Long, city_ip_array: Array[(String, String, String, String)]): Int = {
    // 定义数组的开始下标
    var start = 0
    // 定义数组的结束下标
    var end = city_ip_array.length - 1
    while (start <= end) {
      // 获取一个中间下标
      val middle = (start + end) / 2

      if (ipNum >= city_ip_array(middle)._1.toLong && ipNum <= city_ip_array(middle)._2.toLong) {
        return middle
      }

      if (ipNum < city_ip_array(middle)._1.toLong) {
        end = middle - 1
      }

      if (ipNum > city_ip_array(middle)._2.toLong) {
        start = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")
    // 2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、加载城市ip信息数据，获取 (ip的开始数字、ip的结束数字、经度、纬度)
    val city_ip_rdd: RDD[(String, String, String, String)] = sc.textFile("ip.txt").map(_.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    // 使用spark的广播变量把共同的数据广播到参与计算的worker节点的executor进程中
    val cityIpBbroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(city_ip_rdd.collect())
    // 4、读取运营商日志数据
    val userIpsRDD: RDD[String] = sc.textFile("20090121000132.394251.http.format").map(_.split("\\|")(1))
    // 5、遍历userIpsRDD获取每一个ip地址，然后转换成数字，去广播变量中进行匹配
    val resultRDD: RDD[((String, String), Int)] = userIpsRDD.mapPartitions(iter => {
      // 5.1 获取广播变量的值
      val city_ip_array: Array[(String, String, String, String)] = cityIpBbroadcast.value
      // 5.2 获取每一个ip地址
      iter.map(ip => {
        // 把ip地址转换成数字
        val ipNum = ip2Long(ip)
        // 去广播变量中的数组进行匹配，获取long类型的数字在数组中的下标
        val index: Int = binarySearch(ipNum, city_ip_array)
        // 获取对应下标的信息
        val result: (String, String, String, String) = city_ip_array(index)
        // 封装结果数据，进行返回  ((经度，纬度)，1)
        ((result._3, result._4), 1)
      })
    })
    // 6、相同的经纬度出现的1累加
    val finalResult: RDD[((String, String), Int)] = resultRDD.reduceByKey(_ + _)
    // 7、打印结果数据
    finalResult.foreach(println)

    sc.stop()
  }
}
