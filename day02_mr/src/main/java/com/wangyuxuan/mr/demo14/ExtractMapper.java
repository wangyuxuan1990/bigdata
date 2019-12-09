package com.wangyuxuan.mr.demo14;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/9 10:22
 * @description mapper类
 */
public class ExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if (split.length == 6) {
            context.write(value, NullWritable.get());
        } else {
            context.getCounter("MR_COUNT", "BadRecordCounter").increment(1L);
        }
    }
}
