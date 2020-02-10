package com.wangyuxuan.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author wangyuxuan
 * @date 2020/2/10 3:42 下午
 * @description 开发kafka生产者代码 异步/同步发送模式
 */
public class KafkaProducerStudyDemo {
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
            // 这是异步发送的模式
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        // 消息发送成功
                        System.out.println("消息发送成功");
                        String topic = metadata.topic();
                        int partition = metadata.partition();
                        long offset = metadata.offset();
                        System.out.println("topic:" + topic + "\tpartition:" + partition + "\toffset:" + offset);
                    } else {
                        // 消息发送失败，需要重新发送
                    }
                }
            });

            // 这是同步发送的模式
            // producer.send(record).get();
            // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
            // 有了消息的回应返回给你，你这个方法才会退出来
        }

        producer.close();
    }
}
