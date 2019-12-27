package com.wangyuxuan.hbase.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/27 16:43
 * @description HDFS2HBase
 */
public class HDFS2HBase {

    public static class HdfsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // 数据原样输出
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class HBASEReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
        // key -> 一行数据
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("\t");
            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes(split[1]));
            put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(split[2]));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        // 设定zk集群
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HDFS2HBase.class);

        job.setInputFormatClass(TextInputFormat.class);
        // 输入文件路径
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/hbase/input"));
        // map端的输出的key value 类型
        job.setMapperClass(HdfsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定输出到hbase的表名
        TableMapReduceUtil.initTableReducerJob("myuser2", HBASEReducer.class, job);
        // 设置reduce个数
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
