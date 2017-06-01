package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.pojo.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class DataRepository extends Repository {

    private static final String SQL_MERGE_DATA = ""
            + " MERGE INTO data (         "
            + "            \"metric_id\", "
            + "            \"timestamp\", "
            + "            \"value\")     "
            + " VALUES (?, ?, ?)          ";

    public DataRepository(Supplier<PugConnection> connectionSupplier) {
        super(connectionSupplier);
    }

    public void upsertMetricValue(Metric<?> metric) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_MERGE_DATA)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, new Timestamp(metric.getTimestamp()));
            statement.setBytes(3, metric.getValueAsBytes());
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot upsert metric %s value with statement %s", metric, SQL_MERGE_DATA, e);
        }
    }
}
