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
            + " DELETE FROM data_%s     "
            + " WHERE \"metric_id\" = ? "
            + " AND   \"timestamp\" < ? ";

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

    public void deleteAggregatedDataBeforeTime(String aggregationPeriod, int metricId, long time) {
        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, aggregationPeriod);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setInt(1, metricId);
            statement.setTimestamp(2, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric values with statement %s and ID %s", sql, metricId, e);
        }
    }
//
//    public void deleteDataPer1SecBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1s");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    public void deleteDataPer1MinBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1m");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    public void deleteDataPer1HourBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1h");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    public void deleteDataPer1DayBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1d");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    public void deleteDataPer1MonthBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1mo");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    public void deleteDataPer1YearBeforeTime(int metricId, long time) {
//        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, "1y");
//        deleteDataBeforeTime(sql, metricId, time);
//    }
//
//    private void deleteDataBeforeTime(String sql, int metricId, long time) {
//        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
//            statement.setInt(1, metricId);
//            statement.setTimestamp(2, new Timestamp(time));
//            statement.execute();
//        } catch (SQLException e) {
//            throw new PugSQLException("Cannot delete metric values with statement %s and ID %s", sql, metricId, e);
//        }
//    }
}
