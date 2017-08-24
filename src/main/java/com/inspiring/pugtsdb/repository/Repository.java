package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.sql.PugConnection;

public interface Repository {

    default PugConnection getConnection() {
        return null;
    }
}
