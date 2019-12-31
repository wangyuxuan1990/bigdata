package com.wangyuxuan.hbase.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/31 16:13
 * @description 测试协处理器
 */
public class TestObserver {

    @Test
    public void testPut() throws IOException {
        // 获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table proc1 = connection.getTable(TableName.valueOf("proc1"));

        Put put = new Put("hello_world".getBytes());
        put.addColumn(Bytes.toBytes("info"), "name".getBytes(), "helloworld".getBytes());
        put.addColumn(Bytes.toBytes("info"), "gender".getBytes(), "abc".getBytes());
        put.addColumn(Bytes.toBytes("info"), "nationality".getBytes(), "test".getBytes());
        proc1.put(put);
        byte[] row = put.getRow();
        System.out.println(Bytes.toString(row));
        proc1.close();
        connection.close();
    }
}
