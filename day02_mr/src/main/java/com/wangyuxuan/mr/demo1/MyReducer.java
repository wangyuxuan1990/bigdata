package com.wangyuxuan.mr.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/4 17:32
 * @description reducer类
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable.set(0);
    }

    /**
     * 分区   相同key的数据发送到同一个reduce里面去，相同key合并，value形成一个集合
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable value : values) {
            result += value.get();
        }
        intWritable.set(result);
        context.write(key, intWritable);
    }
}
