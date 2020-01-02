package com.wangyuxuan.hbase.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2020/1/2 11:43
 * @description HBase TTL
 */
public class HBaseVersionAndTTL {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf("version_hbase"))) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("version_hbase"));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("f1");
            hColumnDescriptor.setMinVersions(3);
            hColumnDescriptor.setMaxVersions(5);
            // 针对某一个列族下面所有的列设置TTL
            hColumnDescriptor.setTimeToLive(30);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }
        Table version_hbase = connection.getTable(TableName.valueOf("version_hbase"));
        Put put1 = new Put("1".getBytes());
        put1.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan".getBytes());
        version_hbase.put(put1);
        Thread.sleep(1000);

        Put put2 = new Put("1".getBytes());
        put2.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan2".getBytes());
        version_hbase.put(put2);

        Get get = new Get("1".getBytes());
        get.setMaxVersions();
        Result result = version_hbase.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        version_hbase.close();
        connection.close();
    }
}
