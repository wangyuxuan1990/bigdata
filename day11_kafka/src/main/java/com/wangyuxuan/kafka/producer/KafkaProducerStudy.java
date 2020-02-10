package com.wangyuxuan.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author wangyuxuan
 * @date 2020/2/6 4:06 下午
 * @description 开发kafka生产者代码
 */
public class KafkaProducerStudy {
    public static void main(String[] args) {
        // 准备配置属性
        Properties props = new Properties();
        // kafka集群地址
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        // acks它代表消息确认机制
        props.put("acks", "all");
        // 重试的次数
        props.put("retries", 0);
        // 批处理数据的大小，每次写入多少数据到topic
        props.put("batch.size", 16384);
        // 可以延长多久发送数据
        props.put("linger.ms", 1);
        // 缓冲区的大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 添加自定义分区函数
        props.put("partitioner.class", "com.wangyuxuan.kafka.partitioner.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            // 1、给定具体的分区号，数据就会写入到指定的分区中
//            producer.send(new ProducerRecord<String, String>("test", 0, Integer.toString(i), "hello-kafka-" + i));
            // 2、不给定具体的分区号，给定一个key值 ,这里使用key的 hashcode%分区数=分区号
            // 如果想要指定一个key值，这里就需要key是不断变化
//            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-" + i));
            // 3、不给定具体的分区号，也不给定对应的key ,这个它会进行轮训的方式把数据写入到不同分区中
//            producer.send(new ProducerRecord<String, String>("test", "hello-kafka-" + i));
            // 4、自定义分区函数
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-" + i));
        }

        producer.close();
    }
}
