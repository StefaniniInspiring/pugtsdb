package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.pojo.Metric;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class MetricRepository extends Repository {

    static final String FIELD_ID = "id";
    static final String FIELD_NAME = "name";
    static final String FIELD_TYPE = "type";

    private static final String SQL_SELECT_METRIC_BY_ID = ""
            + " SELECT \"" + FIELD_ID + "\",    "
            + "        \"" + FIELD_NAME + "\",  "
            + "        \"" + FIELD_TYPE + "\"   "
            + " FROM   metric "
            + " WHERE  \"" + FIELD_ID + "\" = ? ";

    private static final String SQL_INSERT_METRIC = ""
            + " INSERT INTO metric ( "
            + "        \"" + FIELD_ID + "\",    "
            + "        \"" + FIELD_NAME + "\",  "
            + "        \"" + FIELD_TYPE + "\" )  "
            + " VALUES (?, ?, ?) ";

    private static final String SQL_INSERT_METRIC_TAG = ""
            + " INSERT INTO metric_tag (   "
            + "             \"metric_id\", "
            + "             \"tag_name\",  "
            + "             \"tag_value\") "
            + " VALUES (?, ?, ?)           ";

    private final TagRepository tagRepository;

    public MetricRepository(Supplier<Connection> connectionSupplier) {
        super(connectionSupplier);
        this.tagRepository = new TagRepository(connectionSupplier);
    }

    public boolean notExistsMetric(Integer id) throws SQLException {
        return !existsMetric(id);
    }

    public boolean existsMetric(Integer id) throws SQLException {
        PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_METRIC_BY_ID);
        statement.setInt(1, id);

        try {
            return statement.executeQuery().first();
        } finally {
            statement.close();
        }
    }

    public Metric<?> selectMetric(Integer id) throws SQLException {
        PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_METRIC_BY_ID);
        statement.setInt(1, id);
        ResultSet resultSet = statement.executeQuery();

        return buildMetric(resultSet);
    }

    public void insertMetric(Metric<?> metric) throws SQLException {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_INSERT_METRIC)) {
            statement.setInt(1, metric.getId());
            statement.setString(2, metric.getName());
            statement.setString(3, metric.getClass().getName());
            statement.execute();
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
                    e.printStackTrace();
                }
            });
        }
    }

    private Metric<?> buildMetric(ResultSet resultSet) throws SQLException {
        Metric<?> metric = null;

        if (resultSet.first()) {
            try {
                String type = resultSet.getString(FIELD_TYPE);
                String name = resultSet.getString(FIELD_NAME);
                Map<String, String> tags = Collections.emptyMap();//TODO select tags
                metric = (Metric<?>) Class.forName(type)
                        .getConstructor(String.class, Map.class)
                        .newInstance(name, tags);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return metric;
    }
}
