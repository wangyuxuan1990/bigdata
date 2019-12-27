package com.wangyuxuan.hbase.demo1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/27 16:15
 * @description reducer类
 * TableReducer第三个泛型包含rowkey信息
 */
public class HBaseWriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {

    /**
     * 将map传输过来的数据，写入到hbase表
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        // rowkey
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        immutableBytesWritable.set(Bytes.toBytes(key.toString()));

        //遍历put对象，并输出
        for (Put value : values) {
            context.write(immutableBytesWritable, value);
        }
    }
}
