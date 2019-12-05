package com.wangyuxuan.mr.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/5 17:00
 * @description NlineInputFormat实战
 */
public class NLineMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mrdemo5");
        job.setJarByClass(NLineMain.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        NLineInputFormat.addInputPath(job, new Path("file:///"));

        job.setMapperClass(NLineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(NLineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///"));

        job.waitForCompletion(true);

    }

    public static class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            for (String word : split) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0L;
            for (LongWritable value : values) {
                result += value.get();
            }
            context.write(key, new LongWritable(result));
        }
    }
}
