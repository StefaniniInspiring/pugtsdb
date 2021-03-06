package com.inspiring.pugtsdb.repository.h2;

import com.inspiring.pugtsdb.repository.Repository;
import com.inspiring.pugtsdb.sql.PugConnection;
import java.util.function.Supplier;

public abstract class H2Repository implements Repository {

    private Supplier<PugConnection> connectionSupplier;

    H2Repository() {
        super();
    }

    H2Repository(Supplier<PugConnection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    void setConnectionSupplier(Supplier<PugConnection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public PugConnection getConnection() {
        return connectionSupplier.get();
    }
}
