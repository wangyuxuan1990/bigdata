package com.wangyuxuan.flink.demo5

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author wangyuxuan
 * @date 2020/3/19 16:05
 * @description 自定义sink将我们的数据发送到redis里面去
 */
object Stream2Redis {
  def main(args: Array[String]): Unit = {
    // 获取程序入口类
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 组织数据
    val streamSource: DataStream[String] = environment.fromElements("hello world", "key value")
    // 将数据包装成为key,value对形式的tuple
    val tupleValue: DataStream[(String, String)] = streamSource.map(x => (x.split(" ")(0), x.split(" ")(1)))

    val builder: FlinkJedisPoolConfig.Builder = new FlinkJedisPoolConfig.Builder
    builder.setHost("node01")
    builder.setPort(6379)
    builder.setTimeout(5000) // 连接超时时间
    builder.setMaxTotal(50) // 最大的客户端连接数量
    builder.setMaxIdle(10) // 最大客户端空闲数
    builder.setMinIdle(5) // 最小客户端空闲数
    val config: FlinkJedisPoolConfig = builder.build()
    //获取redis  sink
    val redisSink: RedisSink[(String, String)] = new RedisSink[Tuple2[String, String]](config, new MyRedisMapper)

    // 使用我们自定义的sink
    tupleValue.addSink(redisSink)
    // 执行程序
    environment.execute("redisSink")
  }
}

class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
  override def getCommandDescription: RedisCommandDescription = {
    // 使用set命令，将数据写入到redis里面去
    // set key value
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}