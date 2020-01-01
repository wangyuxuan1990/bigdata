package com.wangyuxuan.phoenix.demo1;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.*;

/**
 * @author wangyuxuan
 * @date 2020/1/1 7:11 下午
 * @description PhoenixJDBC
 */
public class PhoenixSearch {

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;


    @BeforeTest
    public void init() throws SQLException {
        // 定义phoenix的连接url地址
        String url = "jdbc:phoenix:node01:2181";
        connection = DriverManager.getConnection(url);
        // 构建Statement对象
        statement = connection.createStatement();
    }

    @Test
    public void queryTable() throws SQLException {
        // 定义查询的sql语句，注意大小写
        String sql = "select * from US_POPULATION";
        // 执行sql语句
        try {
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("state:" + resultSet.getString("state"));
                System.out.println("city:" + resultSet.getString("city"));
                System.out.println("population:" + resultSet.getInt("population"));
                System.out.println("--------------------");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }
}
