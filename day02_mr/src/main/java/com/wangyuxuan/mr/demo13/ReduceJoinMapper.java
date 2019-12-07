package com.wangyuxuan.mr.demo13;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/7 6:49 下午
 * @description mapper类
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if (value.toString().startsWith("p")) {
            context.write(new Text(split[0]), value);
        } else {
            context.write(new Text(split[2]), value);
        }
    }
}
