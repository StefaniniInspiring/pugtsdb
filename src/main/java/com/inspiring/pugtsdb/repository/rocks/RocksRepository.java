package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.repository.Repository;
import com.inspiring.pugtsdb.sql.PugConnection;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.util.DummySqlConnection;
import java.sql.SQLException;
import java.util.Map;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

public class RocksRepository implements Repository {

    public static final String COLUMN_FAMILY_METRIC = "metric";
    public static final String COLUMN_FAMILY_POINT = "point";
    static final char SEP = ':';

    protected RocksDB db;
    protected ColumnFamilyOptions columnFamilyOptions;
    protected Map<String, ColumnFamilyHandle> columnFamilyCache;

    public RocksRepository() {
        super();
    }

    public RocksRepository(RocksDB db,
                           ColumnFamilyOptions columnFamilyOptions,
                           Map<String, ColumnFamilyHandle> columnFamilyCache) {
        setRocksDb(db, columnFamilyOptions, columnFamilyCache);
    }

    public void setRocksDb(RocksDB db, ColumnFamilyOptions columnFamilyOptions, Map<String, ColumnFamilyHandle> columnFamilyCache) {
        this.db = db;
        this.columnFamilyOptions = columnFamilyOptions;
        this.columnFamilyCache = columnFamilyCache;
    }

    protected ColumnFamilyHandle createColumnFamily(String name) {
        try {
            return db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions));
        } catch (Exception e) {
            throw new PugException("Cannot create column family " + name, e);
        }
    }

    protected ColumnFamilyHandle getPointColumnFamily(String aggregation, Granularity granularity) {
        return columnFamilyCache.computeIfAbsent(COLUMN_FAMILY_POINT + SEP + aggregation + SEP + granularity, this::createColumnFamily);
    }

    protected ColumnFamilyHandle getMetricColumnFamily() {
        return columnFamilyCache.get(COLUMN_FAMILY_METRIC);
    }

    @Override
    public PugConnection getConnection() {
        try {
            return new PugConnection(new DummySqlConnection());
        } catch (SQLException e) {
            throw new PugException("Cannot create dummy SQL connection", e);
        }
    }
}
