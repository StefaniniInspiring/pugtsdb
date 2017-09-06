package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.repository.h2.H2Repositories;
import com.inspiring.pugtsdb.rollup.schedule.ScheduledPointPurger;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.sql.PugSQLException;
import com.inspiring.pugtsdb.time.Retention;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public class PugTSDBOverH2 extends PugTSDB {

    private static final String DATABASE_SCRIPT = "pugtsdb.sql";

    private final ThreadLocal<PugConnection> currentConnection;
    private final JdbcConnectionPool dataSource;
    private final ScheduledPointPurger purger;

    public PugTSDBOverH2(String storagePath, String username, String password) {
        this(storagePath, username, password, Retention.of(2, ChronoUnit.MINUTES), Retention.of(1, ChronoUnit.YEARS));
    }

    public PugTSDBOverH2(String storagePath, String username, String password, Retention rawRetention, Retention aggregatedRetention) {
        super(new H2Repositories());

        if (isBlank(storagePath)) {
            throw new PugIllegalArgumentException("Database storage path cannot be null nor empty");
        }

        if (isBlank(username)) {
            throw new PugIllegalArgumentException("Database username cannot be null");
        }

        if (isBlank(password)) {
            throw new PugIllegalArgumentException("Database password cannot be null");
        }

        ((H2Repositories) repositories).setConnectionSupplier(this::getConnection);

        dataSource = initDatabase(storagePath, username, password);

        currentConnection = ThreadLocal.withInitial(() -> {
            try {
                return new PugConnection(dataSource.getConnection());
            } catch (SQLException e) {
                throw new PugSQLException("Cannot open a connection", e);
            }
        });

        purger = new ScheduledPointPurger(repositories, rawRetention, aggregatedRetention);
    }

    private JdbcConnectionPool initDatabase(String storagePath, String username, String password) {
        try {
            JdbcConnectionPool ds = JdbcConnectionPool.create("jdbc:h2:" + storagePath, username, password);

            try (Connection connection = ds.getConnection();
                 Statement statement = connection.createStatement();
                 Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(DATABASE_SCRIPT))) {
                scanner.useDelimiter(";").forEachRemaining(sql -> {
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        throw new PugSQLException("Cannot execute DML %s", sql, e);
                    }
                });
            }

            return ds;
        } catch (Exception e) {
            throw new PugSQLException("Cannot create database", e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private PugConnection getConnection() {
        try {
            if (currentConnection.get().isClosed()) {
                currentConnection.remove();
            }
        } catch (SQLException ignored) {
        }

        return currentConnection.get();
    }

    @Override
    protected void closeConnection() {
        try {
            getConnection().close();
        } finally {
            currentConnection.remove();
        }
    }

    @Override
    protected void commitConnection() {
        getConnection().commit();
    }

    @Override
    protected void rollbackConnection() {
        getConnection().rollback();
    }

    @Override
    public void close() throws Exception {
        rollUpScheduler.close();
        purger.close();
        dataSource.dispose();
    }
}
