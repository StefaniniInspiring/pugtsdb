package com.inspiring.pugtsdb.repository;

import java.sql.Connection;

public interface Repository {

    ThreadLocal<Connection> CONNECTION_THREAD_LOCAL = new ThreadLocal<>();

    default Connection getConnection() {
        return CONNECTION_THREAD_LOCAL.get();
    }

    default void setConnection(Connection connection) {
        CONNECTION_THREAD_LOCAL.set(connection);
    }
}
