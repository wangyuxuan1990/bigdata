package com.wangyuxuan.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyuxuan
 * @date 2019/12/23 14:04
 * @description 开发hbase的javaAPI操作
 */
public class HbaseOperate {

    private Connection connection;
    private final String TABLE_NAME = "myuser";
    private Table table;

    //操作数据库  第一步：获取连接  第二步：获取客户端对象   第三步：操作数据库  第四步：关闭

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        // 获取管理员对象，来对手数据库进行DDL的操作
        Admin admin = connection.getAdmin();
        // 指定表名
        TableName myuser = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);
        // 指定两个列族
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        admin.createTable(hTableDescriptor);
        admin.close();
    }

    /**
     * 向myuser表当中添加数据
     *
     * @throws IOException
     */
    @Test
    public void addData() throws IOException {
        // 创建put对象，并指定rowkey值
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("zhangsan"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("地球人"));
        table.put(put);
    }

    /**
     * hbase的批量插入数据
     *
     * @throws IOException
     */
    @Test
    public void batchInsert() throws IOException {
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        //f1
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        //f2
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        table.put(listPut);
    }

    /**
     * 查询rowkey为0003的人
     *
     * @throws IOException
     */
    @Test
    public void getData() throws IOException {
        // 通过get对象，指定rowkey
        Get get = new Get(Bytes.toBytes("0003"));
        // 限制只查询f1列族下面所有列的值
        get.addFamily("f1".getBytes());
        // 查询f2  列族 phone  这个字段
        get.addColumn("f2".getBytes(), "phone".getBytes());
        // 通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
        Result result = table.get(get);
        // 获取一条数据所有的cell，所有数据值都是在cell里面的
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            // 获取列族名
            byte[] family = CellUtil.cloneFamily(cell);
            // 获取列名
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            // 获取rowkey
            byte[] row = CellUtil.cloneRow(cell);
            // 获取cell值
            byte[] value = CellUtil.cloneValue(cell);
            // 需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
            if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toString(row));
                System.out.println(Bytes.toInt(value));
            } else {
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toString(row));
                System.out.println(Bytes.toString(value));
            }
        }
    }

    /**
     * 不知道rowkey的具体值，我想查询rowkey范围值是0003  到0006
     *
     * @throws IOException
     */
    @Test
    public void scanData() throws IOException {
        // 没有指定startRow以及stopRow  则全表扫描
        Scan scan = new Scan();
        // 只扫描f1列族
        scan.addFamily("f1".getBytes());
        // 扫描 f2列族 phone  这个字段
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());
        // 通过getScanner查询获取到了表里面所有的数据，是多条数据
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询所有的rowkey比0003小的所有的数据
     *
     * @throws IOException
     */
    @Test
    public void rowFilter() throws IOException {
        Scan scan = new Scan();
        // 获取我们比较对象
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        /***
         * rowFilter需要加上两个参数
         * 第一个参数就是我们的比较规则
         * 第二个参数就是我们的比较对象
         */
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        // 为我们的scan对象设置过滤器
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 通过familyFilter来实现列族的过滤
     * 需要过滤，列族名包含f2
     */
    @Test
    public void familyFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("f2");
        // 通过familyfilter来设置列族的过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 列名过滤器 只查询包含name列的值
     *
     * @throws IOException
     */
    @Test
    public void qualifierFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("name");
        // 定义列名过滤器，只查询列名包含name的列
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询哪些字段值  包含数字8
     *
     * @throws IOException
     */
    @Test
    public void contains8() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        // 列值过滤器，过滤列值当中包含数字8的所有的列
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * select  *  from  myuser where name  = '刘备'
     * 会返回我们符合条件数据的所有的字段
     * <p>
     * SingleColumnValueExcludeFilter  列值排除过滤器
     * select  *  from  myuser where name  ！= '刘备'
     *
     * @throws IOException
     */
    @Test
    public void singleColumnValueFilter() throws IOException {
        Scan scan = new Scan();
        // 单列值过滤器，过滤  f1 列族  name  列值为刘备的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询rowkey前缀以  00开头的所有的数据
     *
     * @throws IOException
     */
    @Test
    public void prefixFilter() throws IOException {
        Scan scan = new Scan();
        // 过滤rowkey以  00开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * HBase当中的分页
     *
     * @throws IOException
     */
    @Test
    public void hbasePageFilter() throws IOException {
        int pageNum = 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if (pageNum == 1) {
            // 获取第一页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            // 使用分页过滤器来实现数据的分页
            PageFilter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);
            printlReult(scanner);
        } else {
            String startRow = "";
            // 扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            // 设置第一步往前扫描多少条数据
            scan.setMaxResultSize(scanDatas);
            scan.setStartRow(startRow.getBytes());
            PageFilter pageFilter = new PageFilter(scanDatas);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                // 最后一条数据的rowkey就是我们需要的起始的rowkey
                startRow = Bytes.toString(result.getRow());
            }
            // 设置我们扫描多少条数据
            scan.setMaxResultSize(pageSize);
            // 获取第三页的数据
            scan.setStartRow(startRow.getBytes());
            PageFilter pageFilter1 = new PageFilter(pageSize);
            scan.setFilter(pageFilter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printlReult(scanner1);
        }
    }

    /**
     * 查询  f1 列族  name  为刘备数据值
     * 并且rowkey 前缀以  00开头数据
     *
     * @throws IOException
     */
    @Test
    public void filterList() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 删除数据
     */
    @Test
    public void deleteData() throws IOException {
        Delete delete = new Delete("0003".getBytes());
        table.delete(delete);
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {
        // 获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        // 删除一张表之前，需要先禁用表
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.deleteTable(TableName.valueOf(TABLE_NAME));
        admin.close();
    }

    /**
     * 初始化连接
     *
     * @throws IOException
     */
    @BeforeTest
    public void initTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 连接HBase集群不需要指定HBase主节点的ip地址和端口号
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        // 创建连接对象
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    @AfterTest
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    /**
     * 输出结果
     *
     * @param scanner
     */
    private void printlReult(ResultScanner scanner) {
        // 遍历ResultScanner 得到每一条数据，每一条数据都是封装在result对象里面了
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                // 获取列族名
                byte[] family = CellUtil.cloneFamily(cell);
                // 获取列名
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                // 获取rowkey
                byte[] row = CellUtil.cloneRow(cell);
                // 获取cell值
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(row)
                            + "======数据的列族为" + Bytes.toString(family)
                            + "======数据的列名为" + Bytes.toString(qualifier)
                            + "==========数据的值为" + Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowkey为" + Bytes.toString(row)
                            + "======数据的列族为" + Bytes.toString(family)
                            + "======数据的列名为" + Bytes.toString(qualifier)
                            + "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }
}
