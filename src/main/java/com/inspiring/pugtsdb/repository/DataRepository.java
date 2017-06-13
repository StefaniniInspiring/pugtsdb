package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class DataRepository extends Repository {

    public static final String AGGREGATION_OF_ONE_SECOND = "1s";
    public static final String AGGREGATION_OF_ONE_MINUTE = "1m";
    public static final String AGGREGATION_OF_ONE_HOUR = "1h";
    public static final String AGGREGATION_OF_ONE_DAY = "1d";
    public static final String AGGREGATION_OF_ONE_MONTH = "1mo";
    public static final String AGGREGATION_OF_ONE_YEAR = "1y";

    private static final String SQL_MERGE_DATA = ""
            + " MERGE INTO data (         "
            + "            \"metric_id\", "
            + "            \"timestamp\", "
            + "            \"value\")     "
            + " VALUES (?, ?, ?)          ";

    private static final String SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP = ""
            + " DELETE FROM data        "
            + " WHERE \"timestamp\" < ? ";

    private static final String SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP = ""
            + " DELETE FROM data_%s      "
            + " WHERE \"metric_id\"      "
            + " IN (SELECT \"id\"        "
            + "     FROM   metric        "
            + "     WHERE  \"name\" = ?) "
            + " AND   \"timestamp\" < ?  ";

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

    public void deleteRawDataBeforeTime(long time) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP)) {
            statement.setTimestamp(1, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric values with statement %s", SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP, e);
        }
    }

    public void deleteAggregatedDataBeforeTime(String aggregationPeriod, String metricName, long time) {
        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, aggregationPeriod);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setTimestamp(2, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric %s values with statement %s", metricName, sql, e);
        }
    }
}
