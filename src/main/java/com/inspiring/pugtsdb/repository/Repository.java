package com.inspiring.pugtsdb.repository;

import java.sql.Connection;
import java.util.function.Supplier;

public abstract class Repository {

    private final Supplier<Connection> connectionSupplier;

    public Repository(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    protected Connection getConnection() {
        return connectionSupplier.get();
    }
}
