package com.wangyuxuan.flink.demo2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangyuxuan
 * @date 2020/3/24 10:25 下午
 * @description DataStream转换成为Table
 */
public class DataStream2Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("node01", 9999);
        SingleOutputStreamOperator<User> map = dataStreamSource.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] split = value.split(",");
                User user = new User();
                user.setId(Integer.parseInt(split[0]));
                user.setName(split[1]);
                user.setAge(Integer.parseInt(split[2]));
                return user;
            }
        });
        tableEnvironment.registerDataStream("myUser", map);
        Table table = tableEnvironment.sqlQuery("select * from myUser where age > 20");
        /**
         * 使用append模式将Table转换成为dataStream，不能用于sum，count，avg等操作，只能用于添加数据操作
         */
        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(table, Row.class);
        rowDataStream.print();
        /**
         * 使用retract模式将Table转换成为DataStream
         */
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnvironment.toRetractStream(table, Row.class);
        tuple2DataStream.print();

        tableEnvironment.execute("dataStream2SQL");
    }

    public static class User {
        private Integer id;
        private String name;
        private Integer age;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }
}
