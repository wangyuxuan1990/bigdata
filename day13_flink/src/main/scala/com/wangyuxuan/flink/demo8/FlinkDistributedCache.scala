package com.wangyuxuan.flink.demo8

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author wangyuxuan
 * @date 2020/3/20 15:10
 * @description 分布式缓存
 */
object FlinkDistributedCache {
  def main(args: Array[String]): Unit = {
    // 将缓存文件，拿到每台服务器的本地磁盘进行存储，然后需要获取的时候，直接从本地磁盘文件进行获取
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 1:注册分布式缓存文件
    environment.registerCachedFile("D:\\数据\\advert.csv", "advert")
    val data = environment.fromElements("hello", "flink", "spark", "dataset")
    val result: DataSet[String] = data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile: File = getRuntimeContext.getDistributedCache.getFile("advert")
        val lines: util.List[String] = FileUtils.readLines(myFile)
        val it: util.Iterator[String] = lines.iterator()
        while (it.hasNext) {
          val line: String = it.next()
          println("line:" + line)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).setParallelism(2)
    result.print()
    environment.execute()
  }
}
