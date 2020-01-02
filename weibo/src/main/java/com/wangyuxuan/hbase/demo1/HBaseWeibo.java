package com.wangyuxuan.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2020/1/2 14:26
 * @description weibo
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 */
public class HBaseWeibo {

    /**
     * 微博内容表
     */
    private static final byte[] WEIBO_CONTENT = "weibo:content".getBytes();
    /**
     * 用户关系表
     */
    private static final byte[] WEIBO_RELATION = "weibo:relation".getBytes();
    /**
     * 收件箱表
     */
    private static final byte[] WEIBO_RECEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();

    /**
     * 创建命名空间
     *
     * @throws IOException
     */
    public void createNameSpace() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 生成Admin对象
        Admin admin = connection.getAdmin();
        // admin创建namespace
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo").addConfiguration("creator", "wangyuxuan").build();
        admin.createNamespace(namespaceDescriptor);
        // 关闭连接
        admin.close();
        connection.close();
    }

    /**
     * 创建微博内容表
     *
     * @throws IOException
     */
    public void createTableContent() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 生成Admin对象
        Admin admin = connection.getAdmin();
        // 创建
        if (!admin.tableExists(TableName.valueOf(WEIBO_CONTENT))) {
            HTableDescriptor weibo_content = new HTableDescriptor(TableName.valueOf(WEIBO_CONTENT));
            HColumnDescriptor info = new HColumnDescriptor("info");
            // 指定最小版本、最大版本
            info.setMinVersions(1);
            info.setMaxVersions(1);
            info.setBlockCacheEnabled(true);
            weibo_content.addFamily(info);
            admin.createTable(weibo_content);
        }
        // 关闭连接
        admin.close();
        connection.close();
    }

    /**
     * 创建用户关系表
     *
     * @throws IOException
     */
    public void createTableRelation() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 生成Admin对象
        Admin admin = connection.getAdmin();
        // 创建
        if (!admin.tableExists(TableName.valueOf(WEIBO_RELATION))) {
            HTableDescriptor weibo_relation = new HTableDescriptor(TableName.valueOf(WEIBO_RELATION));
            HColumnDescriptor attends = new HColumnDescriptor("attends");
            // attends
            attends.setMinVersions(1);
            attends.setMaxVersions(1);
            attends.setBlockCacheEnabled(true);

            HColumnDescriptor fans = new HColumnDescriptor("fans");
            // fans
            fans.setMinVersions(1);
            fans.setMaxVersions(1);
            fans.setBlockCacheEnabled(true);

            weibo_relation.addFamily(attends);
            weibo_relation.addFamily(fans);
            admin.createTable(weibo_relation);
        }
        // 关闭连接
        admin.close();
        connection.close();
    }

    /**
     * 创建微博收件箱表
     *
     * @throws IOException
     */
    public void createTableReceiveContentEmails() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 生成Admin对象
        Admin admin = connection.getAdmin();
        // 创建
        if (!admin.tableExists(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL))) {
            HTableDescriptor weibo_receive_content_email = new HTableDescriptor(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
            HColumnDescriptor info = new HColumnDescriptor("info");
            // 指定最小版本、最大版本
            info.setMinVersions(1000);
            info.setMaxVersions(1000);
            info.setBlockCacheEnabled(true);
            weibo_receive_content_email.addFamily(info);
            admin.createTable(weibo_receive_content_email);
        }
        // 关闭连接
        admin.close();
        connection.close();
    }

    /**
     * 获取连接
     *
     * @return
     * @throws IOException
     */
    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }


    public static void main(String[] args) throws IOException {
        HBaseWeibo hBaseWeibo = new HBaseWeibo();
        // 建命名空间
//        hBaseWeibo.createNameSpace();
        // 微博内容表
//        hBaseWeibo.createTableContent();
        // 用户关系表
//        hBaseWeibo.createTableRelation();
        // 收件箱表
//        hBaseWeibo.createTableReceiveContentEmails();
    }
}
