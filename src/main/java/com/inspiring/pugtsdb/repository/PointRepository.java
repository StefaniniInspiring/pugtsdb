package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.time.Granularity;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class PointRepository extends Repository {

    private static final String SQL_SELECT_MAX_POINT_TIMESTAMP_BY_NAME_AND_AGGREGATION = ""
            + " SELECT MAX(point.\"timestamp\") AS max     "
            + " FROM   point_%s AS point,                  "
            + "        metric                              "
            + " WHERE  point.\"metric_id\" = metric.\"id\" "
            + " AND    metric.\"name\" = ?                 "
            + " AND    point.\"aggregation\" = ?           ";

    private static final String SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"value\"                     "
            + " FROM   metric,                             "
            + "        point                               "
            + " WHERE  metric.\"name\" = ?                 "
            + " AND    metric.\"id\" = point.\"metric_id\" "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             "
            + " GROUP BY metric.\"id\",                    "
            + "          point.\"timestamp\"               ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"aggregation\",              "
            + "        point.\"value\"                     "
            + " FROM   metric,                             "
            + "        point_%s AS point                   "
            + " WHERE  metric.\"name\" = ?                 "
            + " AND    metric.\"id\" = data.\"metric_id\"  "
            + " AND    point.\"aggregation\" = ?           "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             "
            + " GROUP BY metric.\"id\"                     ";

    private static final String SQL_MERGE_RAW_POINT = ""
            + " MERGE               "
            + " INTO point (        "
            + "      \"metric_id\", "
            + "      \"timestamp\", "
            + "      \"value\")     "
            + " VALUES (?, ?, ?)    ";

    private static final String SQL_MERGE_POINT = ""
            + " MERGE                  "
            + " INTO  point_%s (       "
            + "       \"metric_id\",   "
            + "       \"timestamp\",   "
            + "       \"aggregation\", "
            + "       \"value\")       "
            + " VALUES (?, ?, ?, ?)    ";

    private static final String SQL_DELETE_RAW_POINT_BEFORE_TIMESTAMP = ""
            + " DELETE                   "
            + " FROM   point             "
            + " WHERE  \"timestamp\" < ? ";

    private static final String SQL_DELETE_POINT_BEFORE_TIMESTAMP = ""
            + " DELETE                       "
            + " FROM   point_%s              "
            + " WHERE  \"metric_id\"         "
            + " IN     (SELECT \"id\"        "
            + "         FROM   metric        "
            + "         WHERE  \"name\" = ?) "
            + " AND   \"aggregation\" = ?    "
            + " AND   \"timestamp\" < ?      ";

    public PointRepository(Supplier<PugConnection> connectionSupplier) {
        super(connectionSupplier);
    }

    public Long selectMaxPointTimestampByNameAndAggregation(String metricName, String aggregation, Granularity granularity) {
        String sql = String.format(SQL_SELECT_MAX_POINT_TIMESTAMP_BY_NAME_AND_AGGREGATION, granularity);
        Long max = null;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                max = resultSet.getLong("max");
            }
        } catch (SQLException e) {
            throw new PugSQLException("Cannot select max timestamp of metric %s point aggregated as %s with granulairty %s and statment %s",
                                      metricName,
                                      aggregation,
                                      granularity,
                                      sql);
        }

        return max;
    }

    public <T> List<MetricPoints<T>> selectRawMetricPointsByNameBetweenTimestamp(String metricName, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP)) {
            statement.setString(1, metricName);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricPoints(resultSet);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with statement %s",
                                      metricName,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP,
                                      e);
        }
    }

    public <T> List<MetricPoints<T>> selectMetricPointsByNameAndAggregationBetweenTimestamp(String metricName,
                                                                                            String aggregation,
                                                                                            Granularity granularity,
                                                                                            long fromInclusiveTimestamp,
                                                                                            long toExclusiveTimestamp) {
        String sql = String.format(SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP, granularity);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, granularity.toString());
            statement.setTimestamp(3, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(4, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricPoints(resultSet);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points aggregated as %s between %s and %s with granularity %s and statement %s",
                                      metricName,
                                      aggregation,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      granularity,
                                      sql,
                                      e);
        }
    }

    public <T> void upsertMetricPoint(MetricPoint<T> metricPoint) {
        Metric<T> metric = metricPoint.getMetric();
        Point<T> point = metricPoint.getPoint();

        try (PreparedStatement statement = getConnection().prepareStatement(SQL_MERGE_RAW_POINT)) {
            statement.setInt(1, metric.getId());
            statement.setTimestamp(2, new Timestamp(point.getTimestamp()));
            statement.setBytes(3, metric.valueToBytes(point.getValue()));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot upsert metric point %s value with statement %s", metricPoint, SQL_MERGE_RAW_POINT, e);
        }
    }

    public <T> void upsertMetricPoints(MetricPoints<T> metricPoints, Granularity granularity) {
        String sql = String.format(SQL_MERGE_POINT, granularity);
        Metric<T> metric = metricPoints.getMetric();

        metricPoints.getPoints()
                .forEach((aggregation, point) -> point
                        .forEach((timestamp, value) -> {
                            try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
                                statement.setInt(1, metric.getId());
                                statement.setTimestamp(2, new Timestamp(timestamp));
                                statement.setString(3, aggregation);
                                statement.setBytes(4, metric.valueToBytes(value));
                                statement.execute();
                            } catch (SQLException e) {
                                throw new PugSQLException("Cannot upsert metric %s point %s aggregated as %s with statement %s",
                                                          metric,
                                                          Point.of(timestamp, value),
                                                          aggregation,
                                                          sql,
                                                          e);
                            }
                        }));
    }

    public void deleteRawPointsBeforeTime(long time) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_DELETE_RAW_POINT_BEFORE_TIMESTAMP)) {
            statement.setTimestamp(1, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric points before %s with statement %s", time, SQL_DELETE_RAW_POINT_BEFORE_TIMESTAMP, e);
        }
    }

    public void deletePointsByNameAndAggregationBeforeTime(String metricName, String aggregation, Granularity granularity, long time) {
        String sql = String.format(SQL_DELETE_POINT_BEFORE_TIMESTAMP, granularity);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            statement.setTimestamp(3, new Timestamp(time));
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot delete metric %s points aggregated as %s with granularity %s before %s with statement %s",
                                      metricName,
                                      aggregation,
                                      granularity,
                                      time,
                                      sql,
                                      e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> List<MetricPoints<T>> buildMetricPoints(ResultSet resultSet) throws Exception {
        List<MetricPoints<T>> allPoints = new ArrayList<>();
        MetricPoints points = null;
        String aggregation;

        while (resultSet.next()) {
            Integer id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String type = resultSet.getString("type");
            Long timestamp = resultSet.getTimestamp("timestamp").getTime();
            byte[] bytes = resultSet.getBytes("value");
            Map<String, String> tags = Collections.emptyMap();//TODO select tags?

            try {
                aggregation = resultSet.getString("aggregation");
            } catch (SQLException e) {
                aggregation = null;
            }

            if (points == null || !id.equals(points.getMetric().getId())) {
                Metric<T> metric = (Metric<T>) Class.forName(type)
                        .getConstructor(String.class, Map.class)
                        .newInstance(name, tags);
                points = new MetricPoints(metric);
                allPoints.add(points);
            }

            points.put(aggregation, timestamp, bytes);
        }

        return allPoints;
    }
}
