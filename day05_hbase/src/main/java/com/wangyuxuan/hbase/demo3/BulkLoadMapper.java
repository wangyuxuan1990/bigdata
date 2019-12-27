package com.wangyuxuan.hbase.demo3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/27 17:28
 * @description BulkLoadMapper
 * 四个泛型中后两个，分别对应rowkey及put
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        // 封装输出的rowkey类型
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(split[0]));
        // 构建put对象
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());
        context.write(immutableBytesWritable, put);
    }
}
