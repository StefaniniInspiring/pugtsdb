package com.inspiring.pugtsdb.repository.h2;

import com.inspiring.pugtsdb.exception.PugNotImplementedException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.TagRepository;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class MetricH2Repository extends H2Repository implements MetricRepository {

    private static final String SQL_SELECT_METRIC_BY_ID = ""
            + " SELECT \"id\",    "
            + "        \"name\",  "
            + "        \"type\"   "
            + " FROM   metric     "
            + " WHERE  \"id\" = ? ";

    private static final String SQL_SELECT_METRICS_BY_NAME = ""
            + " SELECT \"id\",      "
            + "        \"name\",    "
            + "        \"type\"     "
            + " FROM   metric       "
            + " WHERE  \"name\" = ? ";

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

    private final TagRepository tagRepository;

    public MetricH2Repository(Supplier<PugConnection> connectionSupplier, TagRepository tagRepository) {
        super(connectionSupplier);
        this.tagRepository = tagRepository;
    }

    public MetricH2Repository(TagRepository tagRepository) {
        this.tagRepository = tagRepository;
    }

    @Override
    public boolean notExistsMetric(Metric<?> metric) {
        return !existsMetric(metric);
    }

    @Override
    public boolean existsMetric(Metric<?> metric) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_METRIC_BY_ID)) {
            statement.setString(1, metric.getId());

            return statement.executeQuery().first();
        } catch (SQLException e) {
            throw new PugSQLException("Cannot check metric %s existence with query %s", metric.getId(), SQL_SELECT_METRIC_BY_ID, e);
        }
    }

    @Override
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

    @Override
    public <T> Metric<T> selectMetricById(String id) {
        throw new PugNotImplementedException(getClass().getSimpleName() + ".selectMetricById(String id) not implemented yet");
    }

    @Override
    public List<Metric<Object>> selectMetricsByName(String name) {
        List<Metric<Object>> metrics = new ArrayList<>();

        try (PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_METRICS_BY_NAME)) {
            statement.setString(1, name);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                metrics.add(buildMetric(resultSet));
            }
        } catch (Exception e) {
            throw new PugSQLException("Cannot select metrics by name %s with statement %s", name, SQL_SELECT_METRICS_BY_NAME, e);
        }

        return metrics;
    }

    @Override
    public void insertMetric(Metric<?> metric) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_INSERT_METRIC)) {
            statement.setString(1, metric.getId());
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
                    statement.setString(1, metric.getId());
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

    @SuppressWarnings("unchecked")
    private <T> Metric<T> buildMetric(ResultSet resultSet) throws Exception {
        String id = resultSet.getString("id");
        String name = resultSet.getString("name");
        String type = resultSet.getString("type");
        Map<String, String> tags = tagRepository.selectTagsByMetricId(id);

        return (Metric<T>) Class.forName(type)
                .getConstructor(String.class, Map.class)
                .newInstance(name, tags);
    }
}
