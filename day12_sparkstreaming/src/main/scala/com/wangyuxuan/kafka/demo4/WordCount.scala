//package com.wangyuxuan.kafka.demo4
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * @author wangyuxuan
// * @date 2020/3/4 14:31
// * @description 与Kafka整合数据不丢失方案 0.8版本
// */
//object WordCount {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    // 步骤一：建立程序入口
//    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
//
//    // 步骤二：设置各种参数
//    val brokers: String = "node01:9092,node02:9092,node03:9092"
//    val topics: String = "sparkstreaming"
//    // 注意，这个也就是我们的消费者的名字
//    val groupId: String = "test4"
//    val topicSet: Set[String] = topics.split(",").toSet
//
//    val kafkaParams: Map[String, String] = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "group.id" -> groupId,
//      "auto.commit.enable" -> "false"
//    )
//
//    // 关键步骤一：设置监听器，帮我们完成偏移量的提交
//    // 监听器的作用就是，我们每次运行完一个批次，就帮我们提交一次偏移量。
//    ssc.addStreamingListener(new KafkaListener(kafkaParams))
//
//    // 关键步骤二： 创建对象，然后通过这个对象获取到上次的偏移量，然后获取到数据流
//    val km: KafkaManager = new KafkaManager(kafkaParams)
//
//    // 步骤三：创建一个程序入口
//    val messages: InputDStream[(String, String)] = km.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicSet
//    )
//
//    // 完成你的业务逻辑即可
//    // 只要对messages这个对象做一下操作，里面对偏移量信息就会丢失了
//    messages.map(_._2).flatMap(_.split(",")).map((_, 1)).foreachRDD(rdd => {
//      rdd.foreach(line => {
//        println(line)
//        println("==============进行业务处理就可以了============batch=========")
//        //这儿是没有办法获取到偏移量信息的。
//      })
//      println("在这儿提交偏移量")
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//  }
//}
