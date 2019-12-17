package com.wangyuxuan.mr.demo16;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/17 15:34
 * @description 共同好友step1 mapper
 */
public class CommonFriendStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k;

    private Text v;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        v = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        v.set(split[0]);
        for (String friend : split[1].split(",")) {
            k.set(friend);
            context.write(k, v);
        }
    }
}
