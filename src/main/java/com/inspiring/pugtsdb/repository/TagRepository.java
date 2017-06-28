package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class TagRepository extends Repository {

    private static final String SQL_SELECT_TAG_BY_METRIC_ID = ""
            + " SELECT \"metric_id\",    "
            + "        \"tag_name\",     "
            + "        \"tag_value\"     "
            + " FROM   metric_tag        "
            + " WHERE  \"metric_id\" = ? ";

    private static final String SQL_MERGE_TAG = ""
            + " MERGE INTO tag (       "
            + "            \"name\",   "
            + "            \"value\" ) "
            + " VALUES (?, ?)          ";

    public TagRepository(Supplier<PugConnection> connectionSupplier) {
        super(connectionSupplier);
    }

    public Map<String, String> selectTagsByMetricId(int metricId) {
        Map<String, String> tags = new HashMap<>();

        try (PreparedStatement statement = getConnection().prepareStatement(SQL_SELECT_TAG_BY_METRIC_ID)) {
            statement.setInt(1, metricId);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                tags.put(resultSet.getString("tag_name"), resultSet.getString("tag_value"));
            }
        } catch (SQLException e) {
            throw new PugSQLException("Cannot select tags for metric %s with statement %s", metricId, SQL_SELECT_TAG_BY_METRIC_ID, e);
        }

        return tags;
    }

    public void upsertTags(Map<String, String> tags) {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_MERGE_TAG)) {
            tags.forEach((name, value) -> {
                try {
                    statement.setString(1, name);
                    statement.setString(2, value);
                    statement.execute();
                } catch (SQLException e) {
                    throw new PugSQLException("Cannot upsert tag %s=%s with statement %s", name, value, SQL_MERGE_TAG, e);
                }
            });
        } catch (SQLException e) {
            throw new PugSQLException("Cannot upsert tags %s with statement %s", tags, SQL_MERGE_TAG, e);
        }
    }
}
