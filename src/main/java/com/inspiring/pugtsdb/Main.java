package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.pojo.LongMetric;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import javax.sql.DataSource;

public class Main {

    public static void main(String[] args) throws SQLException {
        PugTSDB pugTSDB = new PugTSDB("jdbc:h2:/tmp/pugtsdb", "na", "na");

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "val1");

        LongMetric metric = new LongMetric("metrica-long", tags);

        pugTSDB.upsert(metric);
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
