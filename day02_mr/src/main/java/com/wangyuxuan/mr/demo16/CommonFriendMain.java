package com.wangyuxuan.mr.demo16;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author wangyuxuan
 * @date 2019/12/17 16:13
 * @description 共同好友问题
 */
public class CommonFriendMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job1 = Job.getInstance(super.getConf(), "CommonFriendStep1");
        job1.setJarByClass(CommonFriendMain.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, new Path("file:///"));

        job1.setMapperClass(CommonFriendStepOneMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(CommonFriendStepOneReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("file:///"));

        System.out.println(job1.waitForCompletion(true) ? -1 : 1);

        Job job2 = Job.getInstance(super.getConf(), "CommonFriendStep2");
        job2.setJarByClass(CommonFriendMain.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, new Path("file:///"));

        job2.setMapperClass(CommonFriendStepTwoMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(CommonFriendStepTwoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("file:///"));

        System.out.println(job2.waitForCompletion(true) ? -2 : 2);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new CommonFriendMain(), args);
        System.exit(run);
    }
}
