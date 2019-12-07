package com.wangyuxuan.mr.demo10;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author wangyuxuan
 * @date 2019/12/6 16:52
 * @description GroupPartition
 */
public class GroupPartition extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        // 注意这里：使用orderId作为分区的条件，来进行判断，保证相同的orderId进入到同一个reduceTask里面去
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
