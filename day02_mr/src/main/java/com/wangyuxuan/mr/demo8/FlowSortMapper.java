package com.wangyuxuan.mr.demo8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/5 10:53
 * @description mapper类
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {

    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];
        String upFlow = split[1];
        String downFlow = split[2];
        String upCountFlow = split[3];
        String downCountFlow = split[4];
        flowSortBean.setPhoneNum(phoneNum);
        flowSortBean.setUpFlow(Integer.parseInt(upFlow));
        flowSortBean.setDownFlow(Integer.parseInt(downFlow));
        flowSortBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowSortBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(flowSortBean, NullWritable.get());
    }
}
