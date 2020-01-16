package com.wangyuxuan.impala.demo1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author wangyuxuan
 * @date 2020/1/16 11:03
 * @description ImpalaJdbc
 */
public class ImpalaJdbc {
    public static void main(String[] args) throws Exception {
        // 定义连接驱动类，以及连接url和执行的sql**语句
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String driverUrl = "jdbc:hive2://node03:21050/default;auth=noSasl";
        String sql = "select * from employee";
        // 通过反射加载数据库连接驱动
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(driverUrl);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        // 通过查询，得到数据一共有多少列
        int columnCount = resultSet.getMetaData().getColumnCount();
        // 遍历结果集
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        preparedStatement.close();
        connection.close();
    }
}
