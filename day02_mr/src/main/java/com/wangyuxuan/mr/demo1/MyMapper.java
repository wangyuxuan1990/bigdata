package com.wangyuxuan.mr.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义mapper类需要继承Mapper，有四个泛型，
 * keyin: k1   行偏移量 Long
 * valuein: v1   一行文本内容   String
 * keyout: k2   每一个单词   String
 * valueout : v2   1         int
 *
 * @author wangyuxuan
 * @date 2019/12/4 17:06
 * @description mapper类
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text text;
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
        text = new Text();
        intWritable = new IntWritable(1);
    }

    /**
     * 继承mapper之后，覆写map方法，每次读取一行数据，都会来调用一下map方法
     *
     * @param key：对应k1
     * @param value：对应v1
     * @param context：上下文对象。承上启下，承接上面步骤发过来的数据，通过context将数据发送到下面的步骤里面去
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        for (String word : split) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
