package com.wangyuxuan.mr.demo16;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author wangyuxuan
 * @date 2019/12/17 15:36
 * @description 共同好友step2 reducer
 */
public class CommonFriendStepTwoReducer extends Reducer<Text, Text, Text, NullWritable> {

    private Text text;

    private NullWritable nullWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        nullWritable = NullWritable.get();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        Set<String> set = new TreeSet<>();
        for (Text friend : values) {
            set.add(friend.toString());
        }
        for (String s : set) {
            sb.append(s).append(",");
        }
        text.set(key.toString() + ":" + sb.deleteCharAt(sb.length() - 1).toString());
        context.write(text, nullWritable);
    }
}
