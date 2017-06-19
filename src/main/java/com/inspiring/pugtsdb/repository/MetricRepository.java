package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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
}
