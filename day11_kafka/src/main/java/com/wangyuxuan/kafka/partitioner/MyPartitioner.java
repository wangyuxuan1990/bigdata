package com.wangyuxuan.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author wangyuxuan
 * @date 2020/2/10 3:32 下午
 * @description 自定义kafka的分区函数
 */
public class MyPartitioner implements Partitioner {
    /**
     * 通过这个方法来实现消息要去哪一个分区中
     *
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取topic分区数
        int partitions = cluster.partitionsForTopic(topic).size();
        // key.hashCode()可能会出现负数 -1 -2 0 1 2
        // Math.abs 取绝对值
        return Math.abs(key.hashCode() % partitions);
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
