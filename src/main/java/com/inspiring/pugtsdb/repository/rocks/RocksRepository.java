package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.repository.Repository;
import com.inspiring.pugtsdb.time.Granularity;
import java.util.Map;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import static com.inspiring.pugtsdb.util.Serializer.serialize;

public class RocksRepository implements Repository {

    static final String COLUMN_FAMILY_VALUE_BY_TIME = "value_by_time";
    static final String COLUMN_FAMILY_CLASS_BY_NAME = "class_by_name";
    static final String COLUMN_FAMILY_IDS_BY_TAG = "ids_by_tag";

    protected final RocksDB db;
    protected final ColumnFamilyOptions columnFamilyOptions;
    protected final Map<String, ColumnFamilyHandle> columnFamilyCache;

    public RocksRepository(RocksDB db,
                           ColumnFamilyOptions columnFamilyOptions,
                           Map<String, ColumnFamilyHandle> columnFamilyCache) {
        this.db = db;
        this.columnFamilyOptions = columnFamilyOptions;
        this.columnFamilyCache = columnFamilyCache;
    }

    protected ColumnFamilyHandle createColumnFamily(String name) {
        try {
            return db.createColumnFamily(new ColumnFamilyDescriptor(serialize(name), columnFamilyOptions));
        } catch (Exception e) {
            throw new PugException("Cannot create column family " + name, e);
        }
    }

    protected ColumnFamilyHandle getOrCreateValueByTimeColumnFamily(Integer metricId, String aggregation, Granularity granularity){
        return columnFamilyCache.computeIfAbsent(COLUMN_FAMILY_VALUE_BY_TIME + ':' + metricId + ':' + aggregation + ':' + granularity, this::createColumnFamily);
    }

    protected ColumnFamilyHandle getOrCreateClassByNameColumnFamily() {
        return columnFamilyCache.computeIfAbsent(COLUMN_FAMILY_CLASS_BY_NAME, this::createColumnFamily);
    }

    protected ColumnFamilyHandle getOrCreateIdsByTagColumnFamily(String metricName) {
        return columnFamilyCache.computeIfAbsent(COLUMN_FAMILY_IDS_BY_TAG + ':' + metricName, this::createColumnFamily);
    }
}
