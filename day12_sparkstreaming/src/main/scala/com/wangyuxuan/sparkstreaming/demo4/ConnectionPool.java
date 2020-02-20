package com.wangyuxuan.sparkstreaming.demo4;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author wangyuxuan
 * @date 2020/2/19 17:22
 * @description 连接池
 */
public class ConnectionPool {
    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();
    static {
        dataSource.setJdbcUrl("jdbc:mysql://node03:3306/test");//设置连接数据库的URL

        dataSource.setUser("root");//设置连接数据库的用户名

        dataSource.setPassword("123456");//设置连接数据库的密码

        dataSource.setMaxPoolSize(40);//设置连接池的最大连接数

        dataSource.setMinPoolSize(2);//设置连接池的最小连接数

        dataSource.setInitialPoolSize(10);//设置连接池的初始连接数

        dataSource.setMaxStatements(100);//设置连接池的缓存Statement的最大数
    }

    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
