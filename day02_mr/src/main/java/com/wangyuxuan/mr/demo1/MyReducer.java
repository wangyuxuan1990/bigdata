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

    private IntWritable intWritable;

    /**
     * 初始化方法
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
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
        // result局部变量不能放在setup初始化方法当中，否则计数错误（为累加结果），因为reduce方法是按分组进行调用。
        int result = 0;
        for (IntWritable value : values) {
            result += value.get();
        }
        intWritable.set(result);
        context.write(key, intWritable);
    }
}
