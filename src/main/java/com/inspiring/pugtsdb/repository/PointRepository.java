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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.stream.IntStream.range;

@SuppressWarnings({"SqlNoDataSourceInspection", "Duplicates"})
public class PointRepository extends Repository {

    private static final String SQL_SELECT_MAX_POINT_TIMESTAMP_BY_NAME_AND_AGGREGATION = ""
            + " SELECT MAX(point.\"timestamp\") AS max     "
            + " FROM   point_%s AS point,                  "
            + "        metric                              "
            + " WHERE  point.\"metric_id\" = metric.\"id\" "
            + " AND    metric.\"name\" = ?                 "
            + " AND    point.\"aggregation\" = ?           ";

    private static final String SQL_SELECT_RAW_METRIC_POINTS_BY_ID_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point                           "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"id\" = ?                   "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point                           "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"name\" = ?                 "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_ID_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"aggregation\",              "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point_%s AS point               "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"id\" = ?                   "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_ID_AND_AGGREGATION_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"aggregation\",              "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point_%s AS point               "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"id\" = ?                   "
            + " AND    point.\"aggregation\" = ?           "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"aggregation\",              "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point_%s AS point               "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"name\" = ?                 "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                      "
            + "        metric.\"name\",                    "
            + "        metric.\"type\",                    "
            + "        point.\"timestamp\",                "
            + "        point.\"aggregation\",              "
            + "        point.\"value\"                     "
            + " FROM   metric                              "
            + " INNER JOIN point_%s AS point               "
            + " ON     metric.\"id\" = point.\"metric_id\" "
            + " AND    metric.\"name\" = ?                 "
            + " AND    point.\"aggregation\" = ?           "
            + " AND    point.\"timestamp\" >= ?            "
            + " AND    point.\"timestamp\" < ?             ";

    private static final String SQL_INNER_JOIN_METRIC_TAG = ""
            + " INNER JOIN metric_tag AS t{0}             "
            + " ON     metric.\"id\" = t{0}.\"metric_id\" "
            + " AND    t{0}.\"tag_name\" = ?              "
            + " AND    t{0}.\"tag_value\" = ?             ";

    private static final String SQL_GROUP_BY_METRIC_ID_AND_POINT_AGGREGATION_AND_TIMESTAMP = ""
            + " GROUP BY metric.\"id\",                   "
            + "          point.\"aggregation\",           "
            + "          point.\"timestamp\"               ";

    private static final String SQL_GROUP_BY_METRIC_ID_AND_POINT_TIMESTAMP = ""
            + " GROUP BY metric.\"id\",                    "
            + "          point.\"timestamp\"               ";

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

    private final TagRepository tagRepository;

    public PointRepository(Supplier<PugConnection> connectionSupplier, TagRepository tagRepository) {
        super(connectionSupplier);
        this.tagRepository = tagRepository;
    }

    public Long selectMaxPointTimestampByNameAndAggregation(String metricName, String aggregation, Granularity granularity) {
        String sql = String.format(SQL_SELECT_MAX_POINT_TIMESTAMP_BY_NAME_AND_AGGREGATION, granularity);
        Long max = null;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                Timestamp timestamp = resultSet.getTimestamp("max");

                if (timestamp != null) {
                    max = timestamp.getTime();
                }
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

    public <T> MetricPoints<T> selectRawMetricPointsByIdBetweenTimestamp(int metricId, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        String sql = SQL_SELECT_RAW_METRIC_POINTS_BY_ID_BETWEEN_TIMESTAMP + SQL_GROUP_BY_METRIC_ID_AND_POINT_TIMESTAMP;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setInt(1, metricId);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricPoints(resultSet);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with statement %s",
                                      metricId,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      sql,
                                      e);
        }
    }

    public <T> List<MetricPoints<T>> selectRawMetricsPointsByNameBetweenTimestamp(String metricName, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        String sql = SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP + SQL_GROUP_BY_METRIC_ID_AND_POINT_TIMESTAMP;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricsPoints(resultSet, false);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with statement %s",
                                      metricName,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      sql,
                                      e);
        }
    }

    public <T> List<MetricPoints<T>> selectRawMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                                         Map<String, String> tags,
                                                                                         long fromInclusiveTimestamp,
                                                                                         long toExclusiveTimestamp) {
        StringBuilder sql = new StringBuilder(SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP);
        range(0, tags.size()).forEach(i -> sql.append(MessageFormat.format(SQL_INNER_JOIN_METRIC_TAG, i)));
        sql.append(SQL_GROUP_BY_METRIC_ID_AND_POINT_TIMESTAMP);

        try (PreparedStatement statement = getConnection().prepareStatement(sql.toString())) {
            statement.setString(1, metricName);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));

            int i = 4;

            for (Entry<String, String> tag : tags.entrySet()) {
                statement.setString(i++, tag.getKey());
                statement.setString(i++, tag.getValue());
            }

            ResultSet resultSet = statement.executeQuery();

            return buildMetricsPoints(resultSet, true);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with tags %s and statement %s",
                                      metricName,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      tags,
                                      sql,
                                      e);
        }
    }

    public <T> MetricPoints<T> selectMetricPointsByIdAndAggregationBetweenTimestamp(int metricId,
                                                                                    String aggregation,
                                                                                    Granularity granularity,
                                                                                    long fromInclusiveTimestamp,
                                                                                    long toExclusiveTimestamp) {
        String sql = String.format(SQL_SELECT_METRIC_POINTS_BY_ID_AND_AGGREGATION_BETWEEN_TIMESTAMP, granularity) + SQL_GROUP_BY_METRIC_ID_AND_POINT_TIMESTAMP;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setInt(1, metricId);
            statement.setString(2, aggregation);
            statement.setTimestamp(3, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(4, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricPoints(resultSet);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points aggregated as %s between %s and %s with granularity %s and statement %s",
                                      metricId,
                                      aggregation,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      granularity,
                                      sql,
                                      e);
        }
    }

    public <T> MetricPoints<T> selectMetricPointsByIdBetweenTimestamp(int metricId,
                                                                      Granularity granularity,
                                                                      long fromInclusiveTimestamp,
                                                                      long toExclusiveTimestamp) {
        String sql = String.format(SQL_SELECT_METRIC_POINTS_BY_ID_BETWEEN_TIMESTAMP, granularity) + SQL_GROUP_BY_METRIC_ID_AND_POINT_AGGREGATION_AND_TIMESTAMP;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setInt(1, metricId);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricPoints(resultSet);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with granularity %s and statement %s",
                                      metricId,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      granularity,
                                      sql,
                                      e);
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationBetweenTimestamp(String metricName,
                                                                                             String aggregation,
                                                                                             Granularity granularity,
                                                                                             long fromInclusiveTimestamp,
                                                                                             long toExclusiveTimestamp) {
        String sql = String.format(SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP,
                                   granularity) + SQL_GROUP_BY_METRIC_ID_AND_POINT_AGGREGATION_AND_TIMESTAMP;

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            statement.setTimestamp(3, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(4, new Timestamp(toExclusiveTimestamp));
            ResultSet resultSet = statement.executeQuery();

            return buildMetricsPoints(resultSet, false);
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

    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationAndTagsBetweenTimestamp(String metricName,
                                                                                                    String aggregation,
                                                                                                    Granularity granularity,
                                                                                                    Map<String, String> tags,
                                                                                                    long fromInclusiveTimestamp,
                                                                                                    long toExclusiveTimestamp) {
        StringBuilder sql = new StringBuilder(String.format(SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP, granularity));
        range(0, tags.size()).forEach(i -> sql.append(MessageFormat.format(SQL_INNER_JOIN_METRIC_TAG, i)));
        sql.append(SQL_GROUP_BY_METRIC_ID_AND_POINT_AGGREGATION_AND_TIMESTAMP);

        try (PreparedStatement statement = getConnection().prepareStatement(sql.toString())) {
            statement.setString(1, metricName);
            statement.setString(2, aggregation);
            statement.setTimestamp(3, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(4, new Timestamp(toExclusiveTimestamp));

            int i = 5;

            for (Entry<String, String> tag : tags.entrySet()) {
                statement.setString(i++, tag.getKey());
                statement.setString(i++, tag.getValue());
            }

            ResultSet resultSet = statement.executeQuery();

            return buildMetricsPoints(resultSet, true);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points aggregated as %s between %s and %s with granularity %s tags %s and statement %s",
                                      metricName,
                                      aggregation,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      granularity,
                                      tags,
                                      sql.toString(),
                                      e);
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                                      Granularity granularity,
                                                                                      Map<String, String> tags,
                                                                                      long fromInclusiveTimestamp,
                                                                                      long toExclusiveTimestamp) {
        StringBuilder sql = new StringBuilder(String.format(SQL_SELECT_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP, granularity));
        range(0, tags.size()).forEach(i -> sql.append(MessageFormat.format(SQL_INNER_JOIN_METRIC_TAG, i)));
        sql.append(SQL_GROUP_BY_METRIC_ID_AND_POINT_AGGREGATION_AND_TIMESTAMP);

        try (PreparedStatement statement = getConnection().prepareStatement(sql.toString())) {
            statement.setString(1, metricName);
            statement.setTimestamp(2, new Timestamp(fromInclusiveTimestamp));
            statement.setTimestamp(3, new Timestamp(toExclusiveTimestamp));

            int i = 4;

            for (Entry<String, String> tag : tags.entrySet()) {
                statement.setString(i++, tag.getKey());
                statement.setString(i++, tag.getValue());
            }

            ResultSet resultSet = statement.executeQuery();

            return buildMetricsPoints(resultSet, true);
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metric %s points between %s and %s with granularity %s tags %s and statement %s",
                                      metricName,
                                      fromInclusiveTimestamp,
                                      toExclusiveTimestamp,
                                      granularity,
                                      tags,
                                      sql.toString(),
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
    private <T> List<MetricPoints<T>> buildMetricsPoints(ResultSet resultSet, boolean fetchTags) throws Exception {
        List<MetricPoints<T>> metricsPoints = new ArrayList<>();
        MetricPoints metricPoints = null;
        String aggregation;

        while (resultSet.next()) {
            Integer id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String type = resultSet.getString("type");
            Long timestamp = resultSet.getTimestamp("timestamp").getTime();
            byte[] bytes = resultSet.getBytes("value");
            Map<String, String> tags = fetchTags
                                       ? tagRepository.selectTagsByMetricId(id)
                                       : emptyMap();

            try {
                aggregation = resultSet.getString("aggregation");
            } catch (SQLException e) {
                aggregation = null;
            }

            if (metricPoints == null || !id.equals(metricPoints.getMetric().getId())) {
                Metric<T> metric = (Metric<T>) Class.forName(type)
                        .getConstructor(String.class, Map.class)
                        .newInstance(name, tags);
                metricPoints = new MetricPoints(metric);
                metricsPoints.add(metricPoints);
            }

            metricPoints.put(aggregation, timestamp, bytes);
        }

        return metricsPoints;
    }

    @SuppressWarnings("unchecked")
    private <T> MetricPoints<T> buildMetricPoints(ResultSet resultSet) throws Exception {
        MetricPoints metricPoints = null;
        String aggregation;

        if (resultSet.next()) {
            Integer id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String type = resultSet.getString("type");
            Map<String, String> tags = tagRepository.selectTagsByMetricId(id);
            Metric<T> metric = (Metric<T>) Class.forName(type)
                    .getConstructor(String.class, Map.class)
                    .newInstance(name, tags);
            metricPoints = new MetricPoints(metric);

            do {
                Long timestamp = resultSet.getTimestamp("timestamp").getTime();
                byte[] bytes = resultSet.getBytes("value");

                try {
                    aggregation = resultSet.getString("aggregation");
                } catch (SQLException e) {
                    aggregation = null;
                }

                metricPoints.put(aggregation, timestamp, bytes);
            } while (resultSet.next());
        }

        return metricPoints;
    }
}
