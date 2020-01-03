package com.wangyuxuan.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
     * 发送微博
     * 第一步：将uid微博内容添加到content表
     * content
     * 第二步：从relation表中，获得uid的粉丝有哪些fan_uids
     * ralation
     * 第三步：fan_uids中，每个fan_uid插入数据；uid发送微博时的rowkey
     * email
     *
     * @param uid
     * @param content
     * @throws IOException
     */
    public void publishWeibo(String uid, String content) throws IOException {
        // 第一步：将uid微博内容添加到content表
        Connection connection = getConnection();
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        long timeStamp = System.currentTimeMillis();
        // put -> rowkey -> uid+timestamp
        String rowKey = uid + "_" + timeStamp;
        Put put = new Put(rowKey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), timeStamp, content.getBytes());
        weibo_content.put(put);

        // 第二步：从relation表中，获得uid的粉丝有哪些fan_uids
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());
        Result result = weibo_relation.get(get);
        if (result.isEmpty()) {
            weibo_content.close();
            weibo_relation.close();
            connection.close();
            return;
        }
        List<byte[]> fan_uids = new ArrayList<>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] fan_uid = CellUtil.cloneQualifier(cell);
            fan_uids.add(fan_uid);
        }

        // 第三步：fan_uids中，每个fan_uid插入数据；uid发送微博时的rowkey
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        List<Put> putList = new ArrayList<>();
        for (byte[] fan_uid : fan_uids) {
            Put put1 = new Put(fan_uid);
            put1.addColumn("info".getBytes(), uid.getBytes(), timeStamp, rowKey.getBytes());
            putList.add(put1);
        }
        weibo_email.put(putList);

        // 释放资源
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    /**
     * 添加关注用户，一次可能添加多个关注用户
     * A 关注一批用户 B,C ,D
     * 第一步：A是B,C,D的关注者   在weibo:relations 当中attend列族当中以A作为rowkey，B,C,D作为列名，B,C,D作为列值，保存起来
     * 第二步：B,C,D都会多一个粉丝A  在weibo:relations 当中fans列族当中分别以B,C,D作为rowkey，A作为列名，A作为列值，保存起来
     * 第三步：A需要获取B,C,D 的微博内容存放到 receive_content_email 表当中去，以A作为rowkey，B,C,D作为列名，获取B,C,D发布的微博rowkey，放到对应的列值里面去
     *
     * @param uid
     * @param attends
     * @throws IOException
     */
    public void addAttends(String uid, String... attends) throws IOException {
        // 第一：把uid关注别人的逻辑，写到relation表的attend列族下
        Connection connection = getConnection();
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Put put = new Put(uid.getBytes());
        for (String attend : attends) {
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        weibo_relation.put(put);

        // 第二：要将attends有一个粉丝uid的逻辑，添加到relation表的fans列族下
        for (String attend : attends) {
            Put put1 = new Put(attend.getBytes());
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            weibo_relation.put(put1);
        }

        // 第三：去content表查询attends中，每个人发布微博时的rowkey
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Scan scan = new Scan();
        List<byte[]> rowKeyBytes = new ArrayList<>();
        for (String attend : attends) {
            PrefixFilter prefixFilter = new PrefixFilter((attend + "_").getBytes());
            scan.setFilter(prefixFilter);
            ResultScanner scanner = weibo_content.getScanner(scan);
            // 如果当前被关注人没有发送过微博的话，跳过此次循环
            if (null == scanner) {
                continue;
            }
            for (Result result : scanner) {
                byte[] rowkeyWeiboContent = result.getRow();
                rowKeyBytes.add(rowkeyWeiboContent);
            }
        }

        // 第四：要将uid关注的人attends发布的微博时的rowkey写入到email表
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        if (rowKeyBytes.size() > 0) {
            Put put1 = new Put(uid.getBytes());
            for (byte[] rowkeyWeiboContent : rowKeyBytes) {
                String rowKey = Bytes.toString(rowkeyWeiboContent);
                String[] split = rowKey.split("_");
                put1.addColumn("info".getBytes(), split[0].getBytes(), Long.parseLong(split[1]), rowkeyWeiboContent);
            }
            weibo_email.put(put1);
        }

        // 释放资源
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    /**
     * 取消关注 A取消关注 B,C,D这三个用户
     * 其实逻辑与关注B,C,D相反即可
     * 第一步：在weibo:relation关系表当中，在attends列族当中删除B,C,D这三个列
     * 第二步：在weibo:relation关系表当中，在fans列族当中，以B,C,D为rowkey，查找fans列族当中A这个粉丝，给删除掉
     * 第三步：A取消关注B,C,D,在收件箱中，删除取关的人的微博的rowkey
     *
     * @param uid
     * @param attends
     * @throws IOException
     */
    public void cancelAttends(String uid, String... attends) throws IOException {
        // relation:删除关注的人
        Connection connection = getConnection();
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Delete delete = new Delete(uid.getBytes());
        for (String cancelAttend : attends) {
            delete.addColumn("attends".getBytes(), cancelAttend.getBytes());
        }
        weibo_relation.delete(delete);

        // relation：删除attends的粉丝uid
        for (String cancelAttend : attends) {
            Delete delete1 = new Delete(cancelAttend.getBytes());
            delete.addColumn("fans".getBytes(), uid.getBytes());
            weibo_relation.delete(delete1);
        }

        // email：删除uid中，attends相关的列
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Delete delete1 = new Delete(uid.getBytes());
        for (String attend : attends) {
            // Delete all versions of the specified column. 注意和addColumn区别
            delete1.addColumns("info".getBytes(), attend.getBytes());
        }
        weibo_email.delete(delete1);

        // 释放资源
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    /**
     * 某个用户获取收件箱表内容
     * 例如A用户刷新微博，拉取他所有关注人的微博内容
     * A 从 weibo:receive_content_email  表当中获取所有关注人的rowkey
     * 通过rowkey从weibo:content表当中获取微博内容
     *
     * @param uid
     * @throws IOException
     */
    public void getContent(String uid) throws IOException {
        // 从email表获得uid行的所有的值-> 发送微博时的rowkey
        Connection connection = getConnection();
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Get get = new Get(uid.getBytes());
        get.setMaxVersions(5);
        Result result = weibo_email.get(get);
        Cell[] cells = result.rawCells();
        List<Get> getList = new ArrayList<>();
        for (Cell cell : cells) {
            Get get1 = new Get(CellUtil.cloneValue(cell));
            getList.add(get1);
        }

        // 根据这些rowkey去content表获得微博内容
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Result[] results = weibo_content.get(getList);
        for (Result result1 : results) {
            byte[] weiboContent = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println(Bytes.toString(weiboContent));
        }

        // 释放资源
        weibo_content.close();
        weibo_email.close();
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
        //发送微博
//        hBaseWeibo.publishWeibo("2", "今天天气真好");
        //关注别人
//        hBaseWeibo.addAttends("1", "2", "3", "M");
        //取消关注
//        hBaseWeibo.cancelAttends("1", "M");
        //获得所关注人发表的微博
//        hBaseWeibo.getContent("1");
    }
}
