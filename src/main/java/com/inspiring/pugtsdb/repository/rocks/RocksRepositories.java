package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.repository.TagRepository;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

public class RocksRepositories implements Repositories {

    private final MetricRocksRepository metricRepository;
    private final PointRocksRepository pointRepository;

    public RocksRepositories() {
        metricRepository = new MetricRocksRepository();
        pointRepository = new PointRocksRepository(metricRepository);
    }

    public RocksRepositories(RocksDB db, ColumnFamilyOptions columnFamilyOptions, Map<String, ColumnFamilyHandle> columnFamilyCache) {
        metricRepository = new MetricRocksRepository(db, columnFamilyOptions, columnFamilyCache);
        pointRepository = new PointRocksRepository(db, columnFamilyOptions, columnFamilyCache, metricRepository);
    }

    public void setRocksDb(RocksDB db, ColumnFamilyOptions columnFamilyOptions, Map<String, ColumnFamilyHandle> columnFamilyCache) {
        metricRepository.setRocksDb(db, columnFamilyOptions, columnFamilyCache);
        pointRepository.setRocksDb(db, columnFamilyOptions, columnFamilyCache);
    }

    @Override
    public MetricRepository getMetricRepository() {
        return metricRepository;
    }

    @Override
    public PointRepository getPointRepository() {
        return pointRepository;
    }

    @Override
    public TagRepository getTagRepository() {
        return null;
    }
}
