package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.time.Granularity;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class DataRepository extends Repository {

    private static final String SQL_MERGE_DATA = ""
            + " MERGE               "
            + " INTO data (         "
            + "      \"metric_id\", "
            + "      \"timestamp\", "
            + "      \"value\")     "
            + " VALUES (?, ?, ?)    ";

    private static final String SQL_MERGE_DATA_POINT = ""
            + " MERGE                 "
            + " INTO data_%s (        "
            + "      \"metric_id\",   "
            + "      \"timestamp\",   "
            + "      \"aggregation\", "
            + "      \"value\")       "
            + " VALUES (?, ?, ?, ?)   ";

    private static final String SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP = ""
            + " DELETE                   "
            + " FROM   data              "
            + " WHERE  \"timestamp\" < ? ";

    private static final String SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP = ""
            + " DELETE                       "
            + " FROM   data_%s               "
            + " WHERE  \"metric_id\"         "
            + " IN     (SELECT \"id\"        "
            + "         FROM   metric        "
            + "         WHERE  \"name\" = ?) "
            + " AND   \"aggregation\" = ?    "
            + " AND   \"timestamp\" < ?      ";

    static final String SQL_SELECT_MAX_AGGREGATED_DATA_TIMESTAMP = ""
            + " SELECT MAX(\"timestamp\") AS max "
            + " FROM   data_%s                   "
            + " WHERE  \"metric_id\"             "
            + " IN     (SELECT \"id\"            "
            + "         FROM   metric            "
            + "         WHERE  \"name\" = ?)     "
            + " AND    \"aggregation\" = ?       ";

    public DataRepository(Supplier<PugConnection> connectionSupplier) {
        super(connectionSupplier);
    }

    public Long selectMaxAggregatedDataTimestamp(String metricName, String aggregation, Granularity granularity) {
        String sql = String.format(SQL_SELECT_MAX_AGGREGATED_DATA_TIMESTAMP, granularity);
        Long max = null;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                max = resultSet.getLong("max");
            }
        } catch (SQLException e) {
            throw new PugSQLException("Cannot select max timestamp of metric %s aggregated as %s with granulairty %s and statment %s",
                                      metricName,
                                      aggregation,
                                      granularity,
                                      sql);
        }

        return max;
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

    public void upsertMetricPoints(MetricPoints metricPoints, Granularity granularity) {
        String sql = String.format(SQL_MERGE_DATA_POINT, granularity);
        metricPoints.getValues()
                .forEach((aggregation, point) -> point
                        .forEach((timestamp, value) -> {
                            try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
                                statement.setInt(1, metricPoints.getId());
                                statement.setTimestamp(2, new Timestamp(timestamp));
                                statement.setString(3, aggregation);
                                statement.setBytes(4, ?);
                                statement.execute();
                            } catch (SQLException e) {
                                throw new PugSQLException("Cannot upsert metric %s point at timestamp %s and value %s aggregated as %s with statement %s",
                                                          metricPoints.getName(),
                                                          timestamp,
                                                          value,
                                                          aggregation,
                                                          sql,
                                                          e);
                            }
                        }));
    }

    public void deleteRawDataBeforeTime(long time) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP)) {
            statement.setTimestamp(1, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric values with statement %s", SQL_DELETE_RAW_DATA_BEFORE_TIMESTAMP, e);
        }
    }

    public void deleteAggregatedDataBeforeTime(String metricName, String aggregation, Granularity granularity, long time) {
        String sql = String.format(SQL_DELETE_AGGREGATED_DATA_BEFORE_TIMESTAMP, granularity);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            statement.setTimestamp(3, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric %s values aggregated as %s with granularity %s and statement %s",
                                      metricName,
                                      aggregation,
                                      granularity,
                                      sql,
                                      e);
        }
    }
}
