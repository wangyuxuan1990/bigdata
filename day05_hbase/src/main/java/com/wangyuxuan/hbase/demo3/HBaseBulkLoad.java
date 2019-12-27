package com.wangyuxuan.hbase.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author wangyuxuan
 * @date 2019/12/27 17:33
 * @description BulkLoad
 */
public class HBaseBulkLoad extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(HBaseBulkLoad.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/hbase/input"));

        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(super.getConf());
        Table table = connection.getTable(TableName.valueOf("myuser2"));
        // 使MR可以向myuser2表中，增量增加数据
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf("myuser2")));
        // 数据写回到HDFS，写成HFile -> 所以指定输出格式为HFileOutputFormat2
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.setOutputPath(job, new Path("hdfs://node01:8020/hbase/out_hfile"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        // 设定绑定的zk集群
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        int run = ToolRunner.run(configuration, new HBaseBulkLoad(), args);
        System.exit(run);
    }
}
