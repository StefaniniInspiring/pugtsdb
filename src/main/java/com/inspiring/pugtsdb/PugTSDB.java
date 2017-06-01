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

    private static final String DATABASE_SCRIPT = "pugtsdb.sql";

    private final JdbcConnectionPool ds;
    private final ThreadLocal<Connection> connectionThreadLocal = ThreadLocal.withInitial(this::openConnection);
    private final MetricRepository metricRepository = new MetricRepository(this::getConnection);
    private final DataRepository dataRepository = new DataRepository(this::getConnection);

    public PugTSDB(String storageDir, String username, String password) throws SQLException {
        ds = initDatabase(storageDir, username, password);
    }

    private JdbcConnectionPool initDatabase(String storageDir, String username, String password) throws SQLException {
        JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:" + storageDir, username, password);
        Connection connection = ds.getConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            connection.rollback();
        } finally {
            statement.close();
        }

        return ds;
    }

    public void upsert(Metric<?> metric) {
        try {
            if (metricRepository.notExistsMetric(metric.getId())) {
                metricRepository.insertMetric(metric);
            }

            dataRepository.upsertMetricValue(metric);

            getConnection().commit();
        } catch (SQLException e) {
            try {
                getConnection().rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public void close() {
        ds.dispose();
    }

    private Connection getConnection() {
        return connectionThreadLocal.get();
    }

    private Connection openConnection() {
        Connection connection = null;

        try {
            connection = ds.getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }

    private void closeConnection() {
        try {
            getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connectionThreadLocal.remove();
        }
    }
}
