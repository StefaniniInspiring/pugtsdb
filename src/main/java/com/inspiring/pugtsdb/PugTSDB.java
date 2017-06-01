package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.pojo.Metric;
import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.sql.PugConnection;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import org.h2.jdbcx.JdbcConnectionPool;

public class PugTSDB implements Closeable {

    private static final String DATABASE_SCRIPT = "pugtsdb.sql";

    private final JdbcConnectionPool ds;
    private final ThreadLocal<PugConnection> currentConnection;
    private final MetricRepository metricRepository = new MetricRepository(this::getConnection);
    private final DataRepository dataRepository = new DataRepository(this::getConnection);

    public PugTSDB(String storageDir, String username, String password) {
        ds = initDatabase(storageDir, username, password);
        currentConnection = ThreadLocal.withInitial(() -> {
            try {
                return new PugConnection(ds.getConnection());
            } catch (SQLException e) {
                throw new PugSQLException("Cannot open a connection", e);
            }
        });
    }

    private JdbcConnectionPool initDatabase(String storageDir, String username, String password) {
        try {
            JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:" + storageDir, username, password);

            try (Connection connection = ds.getConnection();
                 Statement statement = connection.createStatement();
                 Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(DATABASE_SCRIPT))) {
                scanner.useDelimiter(";").forEachRemaining(sql -> {
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        throw new PugSQLException("Cannot execute DML %s", sql, e);
                    }
                });
            }

            return ds;
        } catch (Exception e) {
            throw new PugSQLException("Cannot create database", e);
        }
    }

    public void upsert(Metric<?> metric) {
        try {
            if (metricRepository.notExistsMetric(metric.getId())) {
                metricRepository.insertMetric(metric);
            }

            dataRepository.upsertMetricValue(metric);

            getConnection().commit();
        } catch (PugSQLException e) {
            getConnection().rollback();
            throw e;
        } finally {
            closeConnection();
        }
    }

    @Override
    public void close() {
        ds.dispose();
    }

    private PugConnection getConnection() {
        return currentConnection.get();
    }

    private void closeConnection() {
        try {
            getConnection().close();
        } finally {
            currentConnection.remove();
        }
    }
}
