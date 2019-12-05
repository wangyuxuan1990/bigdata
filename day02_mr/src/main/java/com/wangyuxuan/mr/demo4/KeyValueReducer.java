package com.wangyuxuan.mr.demo4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/5 16:23
 * @description reducer类
 */
public class KeyValueReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable writable : values) {
            result += writable.get();
        }
        intWritable.set(result);
        context.write(key, intWritable);
    }
}
