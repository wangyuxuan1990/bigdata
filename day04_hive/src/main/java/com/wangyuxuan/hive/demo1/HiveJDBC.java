package com.wangyuxuan.hive.demo1;

import java.sql.*;

/**
 * @author wangyuxuan
 * @date 2019/12/27 11:09
 * @description HiveJDBC
 */
public class HiveJDBC {

    private static final String url = "jdbc:hive2://node03:10000/myhive";

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        // 获取数据库连接
        Connection connection = DriverManager.getConnection(url, "wangyuxuan", "");
        // 定义查询的sql语句
        String sql = "select * from stu";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                // 获取id字段值
                int id = resultSet.getInt(1);
                // 获取name字段
                String name = resultSet.getString(2);
                System.out.println(id + "\t" + name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
