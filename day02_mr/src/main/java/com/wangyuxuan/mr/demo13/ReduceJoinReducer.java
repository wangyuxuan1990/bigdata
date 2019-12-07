package com.wangyuxuan.mr.demo13;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/7 6:49 下午
 * @description reducer类
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstPart = "";
        String secondPart = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                secondPart = value.toString();
            } else {
                firstPart = value.toString();
            }
        }
        context.write(new Text(firstPart + "\t" + secondPart), NullWritable.get());
    }
}
