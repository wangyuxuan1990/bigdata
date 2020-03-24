package com.wangyuxuan.flink.demo2;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * @author wangyuxuan
 * @date 2020/3/24 9:53 下午
 * @description 开发代码读取CSV文件并查询数据
 */
public class TableCSV {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        // TableAPI的入口类
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(localEnvironment);

        CsvTableSource csvSource = CsvTableSource.builder().field("id", Types.INT())
                .field("name", Types.STRING())
                .field("age", Types.INT())
                .fieldDelimiter(",")
                .ignoreParseErrors()
                .ignoreFirstLine()
                .path("/数据/flinksql.csv")
                .build();
        tEnv.registerTableSource("myUser", csvSource);

        Table sqlTable = tEnv.sqlQuery("select * from myUser where age > 20");

        Table result = tEnv.scan("myUser")
                .filter("age > 10")
                .select("id, name, age");

        tEnv.toRetractStream(result, Row.class).print();
        tEnv.toRetractStream(sqlTable, Row.class).print();
        tEnv.execute("csvTable");
    }
}
