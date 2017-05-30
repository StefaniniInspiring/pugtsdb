package com.inspiring.pugtsdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;

public class Main {

    public static void main(String[] args) throws SQLException {
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create("jdbc:h2:/tmp/pugtsdb", "sa", "sa");
        createDatabase(connectionPool);
    }

    private static void createDatabase(DataSource ds) throws SQLException {
        Connection connection = ds.getConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        new Scanner(ds.getClass().getClassLoader().getResourceAsStream("pugtsdb.sql"))
                .useDelimiter(";")
                .forEachRemaining(sql -> {
                    try {
                        statement.execute(sql);
                        System.out.println(sql + " ok");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });

        connection.commit();
    }
}
