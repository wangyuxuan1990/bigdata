package com.wangyuxuan.hive.demo1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/26 11:14
 * @description mapper类
 */
public class VideoMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text;

    private NullWritable nullWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        nullWritable = NullWritable.get();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String washDatas = VideoUtil.washDatas(value.toString());
        if (washDatas != null) {
            text.set(washDatas);
            context.write(text, nullWritable);
        }
    }
}
