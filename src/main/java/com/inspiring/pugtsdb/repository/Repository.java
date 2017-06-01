package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.sql.PugConnection;
import java.sql.Connection;
import java.util.function.Supplier;

public abstract class Repository {

    private final Supplier<PugConnection> connectionSupplier;

    public Repository(Supplier<PugConnection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    protected Connection getConnection() {
        return connectionSupplier.get();
    }
}
