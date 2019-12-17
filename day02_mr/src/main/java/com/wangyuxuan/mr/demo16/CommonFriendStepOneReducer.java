package com.wangyuxuan.mr.demo16;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/17 15:35
 * @description 共同好友step1 reducer
 */
public class CommonFriendStepOneReducer extends Reducer<Text, Text, Text, Text> {

    private Text value;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        value = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text friend : values) {
            sb.append(friend).append(",");
        }
        value.set(sb.deleteCharAt(sb.length() - 1).toString());
        context.write(key, value);
    }
}
