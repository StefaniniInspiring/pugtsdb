package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.repository.Repository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import static java.util.Collections.synchronizedList;

public class RocksRepository implements Repository {

    protected final RocksDB db;
    protected final List<ColumnFamilyHandle> columnFamilyHandles = synchronizedList(new ArrayList<>());
    protected final List<ColumnFamilyDescriptor> columnFamilyDescriptors = synchronizedList(new ArrayList<>());
    protected final Map<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();

    public RocksRepository() {
        db = null;
    }

    protected ColumnFamilyHandle createColumnFamily(String name) {
        return null;
    }
}
