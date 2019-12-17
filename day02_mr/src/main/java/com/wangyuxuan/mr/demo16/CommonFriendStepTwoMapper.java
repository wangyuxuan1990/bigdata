package com.wangyuxuan.mr.demo16;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author wangyuxuan
 * @date 2019/12/17 15:35
 * @description 共同好友step2 mapper
 */
public class CommonFriendStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k;

    private Text v;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        v = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        v.set(split[0]);
        String[] users = split[1].split(",");
        Arrays.sort(users);
        for (int i = 0; i < users.length - 1; i++) {
            for (int j = i + 1; j < users.length; j++) {
                k.set(users[i] + "-" + users[j]);
                context.write(k, v);
            }
        }
    }
}
