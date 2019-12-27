package com.wangyuxuan.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author wangyuxuan
 * @date 2019/12/27 16:20
 * @description HBaseMR
 */
public class HBaseMR extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(HBaseMR.class);
        // mapper
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("myuser"),
                new Scan(),
                HBaseReadMapper.class,
                Text.class,
                Put.class,
                job
        );
        // reducer
        TableMapReduceUtil.initTableReducerJob(
                "myuser2",
                HBaseWriteReducer.class,
                job
        );

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        // 设定绑定的zk集群
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        int run = ToolRunner.run(configuration, new HBaseMR(), args);
        System.exit(run);
    }
}
