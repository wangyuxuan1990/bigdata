package com.wangyuxuan.hbase.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * @author wangyuxuan
 * @date 2019/12/31 15:49
 * @description RegionObserver协处理器
 */
public class MyProcessor extends BaseRegionObserver {

    /**
     * 插入到proc1表里面的数据，都是封装在put对象里面了，可以解析put对象，获取数据，获取到了数据之后，插入到proc2表里面去
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 涉及到多个版本问题
        List<Cell> cells = put.get("info".getBytes(), "name".getBytes());
        Cell cell = cells.get(0);

        Table proc2 = connection.getTable(TableName.valueOf("proc2"));
        Put put1 = new Put(put.getRow());
        put1.add(cell);
        proc2.put(put1);

        proc2.close();
        connection.close();
    }
}
