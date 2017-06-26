package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.rollup.schedule.RollUpScheduler;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Interval;
import com.inspiring.pugtsdb.time.Retention;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public class PugTSDB implements Closeable {

    private static final String DATABASE_SCRIPT = "pugtsdb.sql";

    private final JdbcConnectionPool ds;
    private final ThreadLocal<PugConnection> currentConnection;
    private final Repositories repositories;
    private final RollUpScheduler rollUpScheduler;

    public PugTSDB(String storagePath, String username, String password) {
        if (isBlank(storagePath)) {
            throw new PugIllegalArgumentException("Database storage path cannot be null nor empty");
        }

        if (isBlank(username)) {
            throw new PugIllegalArgumentException("Database username cannot be null");
        }

        if (isBlank(password)) {
            throw new PugIllegalArgumentException("Database password cannot be null");
        }

        ds = initDatabase(storagePath, username, password);

        currentConnection = ThreadLocal.withInitial(() -> {
            try {
                return new PugConnection(ds.getConnection());
            } catch (SQLException e) {
                throw new PugSQLException("Cannot open a connection", e);
            }
        });

        repositories = new Repositories(this::getConnection);
        rollUpScheduler = new RollUpScheduler(repositories);
    }

    private JdbcConnectionPool initDatabase(String storagePath, String username, String password) {
        try {
            JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:" + storagePath, username, password);

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

    public DataSource getDataSource() {
        return ds;
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, String aggregation, Granularity granularity, Interval interval) {
        return null;
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Granularity granularity, Interval interval) {
        return null;
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Interval interval) {
        return null;
    }

    public List<MetricPoints<?>> selectMetricsPoints(String metricName, String aggregation, Granularity granularity, Interval interval, Tag...tags) {
        return null;
    }

    public List<MetricPoints<?>> selectMetricsPoints(String metricName, Granularity granularity, Interval interval, Tag...tags) {
        return null;
    }

    public List<MetricPoints<?>> selectMetricsPoints(String metricName, Interval interval, Tag...tags) {
        return null;
    }

    public <T> void upsertMetricPoint(Metric<T> metric, Point<T> point) {
        if (metric == null) {
            throw new PugIllegalArgumentException("Cannot upsert a null metric");
        }

        if (point == null) {
            throw new PugIllegalArgumentException("Cannot upsert a null metric point");
        }

        MetricRepository metricRepository = repositories.getMetricRepository();
        PointRepository pointRepository = repositories.getPointRepository();

        try {
            if (metricRepository.notExistsMetric(metric.getId())) {
                metricRepository.insertMetric(metric);
            }

            pointRepository.upsertMetricPoint(MetricPoint.of(metric, point));

            getConnection().commit();
        } catch (PugException e) {
            getConnection().rollback();
            throw e;
        } finally {
            closeConnection();
        }
    }

    public void registerRollUps(String metricName, Aggregation<?> aggregation, Retention retention) {
        rollUpScheduler.registerRollUps(metricName, aggregation, retention);
    }

    public void addRollUpListener(String metricName, Aggregation<?> aggregation, Granularity granularity, RollUpListener listener) {
        rollUpScheduler.addRollUpListener(metricName, aggregation, granularity, listener);
    }

    public RollUpListener removeRollUpListener(String metricName, Aggregation<?> aggregation, Granularity granularity) {
        return rollUpScheduler.removeRollUpListener(metricName, aggregation, granularity);
    }

    @Override
    public void close() {
        ds.dispose();
        rollUpScheduler.stop();
    }

    private PugConnection getConnection() {
        try {
            if (currentConnection.get().isClosed()) {
                currentConnection.remove();
            }
        } catch (SQLException ignored) {
        }

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
