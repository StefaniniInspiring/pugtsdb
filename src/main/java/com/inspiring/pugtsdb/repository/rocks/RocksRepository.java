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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksRepository implements Repository {

    public static final String METRIC_COLUMN_FAMILY = "metric";
    public static final String POINT_COLUMN_FAMILY = "point";
    static final char SEP = ':';

    protected RocksDB db;
    protected ColumnFamilyOptions columnFamilyOptions;
    protected Map<String, ColumnFamilyHandle> columnFamilyCache;
    protected ColumnFamilyHandle defaultColumnFamily;

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
        this.defaultColumnFamily = columnFamilyCache.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY));
    }

    public void compactDB() {
        try {
            db.compactRange();
        } catch (Exception e) {
            throw new PugException("Cannot compact database", e);
        }
    }

    protected ColumnFamilyHandle createColumnFamily(String name) {
        try {
            return db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions));
        } catch (Exception e) {
            throw new PugException("Cannot create column family " + name, e);
        }
    }

    protected ColumnFamilyHandle intoPointColumnFamily(String aggregation, Granularity granularity) {
        return columnFamilyCache.computeIfAbsent(POINT_COLUMN_FAMILY + SEP + aggregation + SEP + granularity, this::createColumnFamily);
    }

    protected ColumnFamilyHandle fromPointColumnFamily(String aggregation, Granularity granularity) {
        return columnFamilyCache.getOrDefault(POINT_COLUMN_FAMILY + SEP + aggregation + SEP + granularity, defaultColumnFamily);
    }

    protected ColumnFamilyHandle intoMetricColumnFamily() {
        return columnFamilyCache.computeIfAbsent(METRIC_COLUMN_FAMILY, this::createColumnFamily);
    }

    protected ColumnFamilyHandle fromMetricColumnFamily() {
        return columnFamilyCache.getOrDefault(METRIC_COLUMN_FAMILY, defaultColumnFamily);
    }

    protected ReadOptions newFastReadOptions() {
        return new ReadOptions().setIgnoreRangeDeletions(true).setVerifyChecksums(false);
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
