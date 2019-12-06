package com.wangyuxuan.mr.demo10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/6 16:47
 * @description mappper
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        orderBean = new OrderBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.valueOf(split[2]));
        context.write(orderBean, NullWritable.get());
    }
}
