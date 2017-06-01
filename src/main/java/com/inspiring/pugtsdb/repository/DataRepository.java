package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.pojo.Metric;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;

@SuppressWarnings("SqlNoDataSourceInspection")
public class DataRepository extends Repository {

    private static final String SQL_MERGE_DATA = ""
            + " MERGE INTO data (         "
            + "            \"metric_id\", "
            + "            \"timestamp\", "
            + "            \"value\")     "
            + " VALUES (?, ?, ?)          ";

    public DataRepository(Supplier<Connection> connectionSupplier) {
        super(connectionSupplier);
    }

    public void upsertMetricValue(Metric<?> metric) throws SQLException {
        Timestamp timestamp = new Timestamp(metric.getTimestamp() != null ? metric.getTimestamp() : currentTimeMillis());

        try (PreparedStatement statement = getConnection().prepareStatement(SQL_MERGE_DATA)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, timestamp);
            statement.setBytes(3, metric.getValueAsBytes());
            statement.execute();
        }
    }
}
