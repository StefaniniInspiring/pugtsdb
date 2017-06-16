package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.time.Granularity;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class MetricRepository extends Repository {

    private static final String SQL_SELECT_METRIC_BY_ID = ""
            + " SELECT \"id\",    "
            + "        \"name\",  "
            + "        \"type\"   "
            + " FROM   metric "
            + " WHERE  \"" + "id" + "\" = ? ";

    private static final String SQL_SELECT_DISTINCT_METRIC_NAMES = ""
            + " SELECT DISTINCT \"name\" "
            + " FROM   metric            ";

    private static final String SQL_INSERT_METRIC = ""
            + " INSERT INTO metric ( "
            + "        \"id\",    "
            + "        \"name\",  "
            + "        \"type\" )  "
            + " VALUES (?, ?, ?) ";

    private static final String SQL_INSERT_METRIC_TAG = ""
            + " INSERT INTO metric_tag (   "
            + "             \"metric_id\", "
            + "             \"tag_name\",  "
            + "             \"tag_value\") "
            + " VALUES (?, ?, ?)           ";

    private static final String SQL_SELECT_RAW_METRIC_POINTS_BY_NAME_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                     "
            + "        metric.\"name\",                   "
            + "        metric.\"type\",                   "
            + "        data.\"timestamp\",                "
            + "        data.\"value\"                     "
            + " FROM   metric,                            "
            + "        data                               "
            + " WHERE  metric.\"name\" = ?                "
            + " AND    metric.\"id\" = data.\"metric_id\" "
            + " AND    data.\"timestamp\" >= ?            "
            + " AND    data.\"timestamp\" < ?             "
            + " GROUP BY metric.\"id\"                    ";

    private static final String SQL_SELECT_METRIC_POINTS_BY_NAME_AND_AGGREGATION_BETWEEN_TIMESTAMP = ""
            + " SELECT metric.\"id\",                     "
            + "        metric.\"name\",                   "
            + "        metric.\"type\",                   "
            + "        data.\"timestamp\",                "
            + "        data.\"aggregation\",              "
            + "        data.\"value\"                     "
            + " FROM   metric,                            "
            + "        data_%s AS data                    "
            + " WHERE  metric.\"name\" = ?                "
            + " AND    metric.\"id\" = data.\"metric_id\" "
            + " AND    data.\"aggregation\" = ?           "
            + " AND    data.\"timestamp\" >= ?            "
            + " AND    data.\"timestamp\" < ?             "
            + " GROUP BY metric.\"id\"                    ";

    private final TagRepository tagRepository;

    public MetricRepository(Supplier<PugConnection> connectionSupplier, TagRepository tagRepository) {
        super(connectionSupplier);
        this.tagRepository = tagRepository;
    }

    public boolean notExistsMetric(Integer id) {
        return !existsMetric(id);
    }

    public boolean existsMetric(Integer id) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_METRIC_BY_ID)) {
            statement.setInt(1, id);

            return statement.executeQuery().first();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot check metric %s existence with query %s", id, SQL_SELECT_METRIC_BY_ID, e);
        }
    }

    public List<String> selectMetricNames() {
        List<String> names = new ArrayList<>();

        try (Statement statement = getConnection().createStatement()) {
            ResultSet resultSet = statement.executeQuery(SQL_SELECT_DISTINCT_METRIC_NAMES);

            while (resultSet.next()) {
                names.add(resultSet.getString("name"));
            }
        } catch (SQLException e) {
            throw new PugSQLException("Cannot select metric names with statement %s", SQL_SELECT_DISTINCT_METRIC_NAMES, e);
        }

        return names;
    }

    public void insertMetric(Metric<?> metric) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_INSERT_METRIC)) {
            statement.setInt(1, metric.getId());
            statement.setString(2, metric.getName());
            statement.setString(3, metric.getClass().getTypeName());
            statement.execute();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot insert metric %s with statement %s", metric, SQL_INSERT_METRIC, e);
        }

        tagRepository.upsertTags(metric.getTags());

        try (PreparedStatement statement = getConnection().prepareStatement(SQL_INSERT_METRIC_TAG)) {
            metric.getTags().forEach((name, value) -> {
                try {
                    statement.setInt(1, metric.getId());
                    statement.setString(2, name);
                    statement.setString(3, value);
                    statement.execute();
                } catch (SQLException e) {
                    throw new PugSQLException("Cannot insert relationship between metric %s and tag %s=%s with statement %s",
                                              metric,
                                              name,
                                              value,
                                              SQL_INSERT_METRIC_TAG,
                                              e);
                }
            });
        } catch (SQLException e) {
            throw new PugSQLException("Cannot insert relationships between metric %s and tags %s with statement %s", metric, metric.getTags(), SQL_INSERT_METRIC_TAG, e);
        }
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

    @SuppressWarnings("unchecked")
    private <T> List<MetricPoints<T>> buildMetricPoints(ResultSet resultSet) throws SQLException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
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
