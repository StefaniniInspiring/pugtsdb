package com.inspiring.pugtsdb.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("SqlNoDataSourceInspection")
public class TagRepository extends Repository {

    private static final String SQL_MERGE_TAG = ""
            + " MERGE INTO tag (       "
            + "            \"name\",   "
            + "            \"value\" ) "
            + " VALUES (?, ?)          ";

    public TagRepository(Supplier<Connection> connectionSupplier) {
        super(connectionSupplier);
    }

    public void upsertTags(Map<String, String> tags) throws SQLException {
        try (PreparedStatement statement = getConnection().prepareStatement(SQL_MERGE_TAG)) {
            tags.forEach((name, value) -> {
                try {
                    statement.setString(1, name);
                    statement.setString(2, value);
                    statement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
