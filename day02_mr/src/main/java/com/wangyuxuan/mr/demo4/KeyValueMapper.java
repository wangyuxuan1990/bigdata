package com.wangyuxuan.mr.demo4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/5 16:23
 * @description mapper类
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable(1);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, intWritable);
    }
}
