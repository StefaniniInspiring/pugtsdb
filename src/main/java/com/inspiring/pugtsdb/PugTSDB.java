package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.pojo.Metric;
import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.repository.MetricRepository;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import org.h2.jdbcx.JdbcConnectionPool;

public class PugTSDB {

    public static final String DATABASE_SCRIPT = "pugtsdb.sql";

    private final JdbcConnectionPool ds;
    private final MetricRepository metricRepository = new MetricRepository();
    private final DataRepository dataRepository = new DataRepository();

    public PugTSDB(String storageDir, String username, String password) throws SQLException {
        ds = initDatabase(storageDir, username, password);
    }

    private JdbcConnectionPool initDatabase(String storageDir, String username, String password) throws SQLException {
        JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:" + storageDir, username, password);
        Connection connection = ds.getConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        new Scanner(getClass().getClassLoader().getResourceAsStream(DATABASE_SCRIPT))
                .useDelimiter(";")
                .forEachRemaining(sql -> {
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });

        connection.commit();

        return ds;
    }

    public void upsert(Metric<?> metric) {
        try {
            metricRepository.setConnection(ds.getConnection());

            if (metricRepository.notExistsMetric(metric.getId())) {
                metricRepository.insertMetric(metric);
            } else {
                dataRepository.upsertMetricValue(metric);
            }

            metricRepository.getConnection().commit();
        } catch (SQLException e) {
            try {
                metricRepository.getConnection().rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            e.printStackTrace();
        }
    }
}
