package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.rocks.bean.MetaMetric;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import static com.inspiring.pugtsdb.util.Serializer.deserialize;
import static com.inspiring.pugtsdb.util.Serializer.serialize;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MetricRocksRepository extends RocksRepository implements MetricRepository {

    private final Map<String, Set<String>> metricIdCache = new ConcurrentHashMap<>();
    private final Map<String, Object> metricNameLock = new ConcurrentHashMap<>();

    public MetricRocksRepository() {
        super();
    }

    public MetricRocksRepository(RocksDB db,
                                 ColumnFamilyOptions columnFamilyOptions,
                                 Map<String, ColumnFamilyHandle> columnFamilyCache) {
        super(db, columnFamilyOptions, columnFamilyCache);
    }

    @Override
    public boolean existsMetric(Metric<?> metric) {
        return getMetricIdsFromCache(metric.getName()).contains(metric.getId());
    }

    @Override
    public List<String> selectMetricNames() {
        List<String> names = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator(getMetricColumnFamily())) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                names.add(deserialize(iterator.key(), String.class));
            }
        }

        return names;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<Metric<T>> selectMetricsByName(String name) {
        try {
            byte[] metaBytes = db.get(getMetricColumnFamily(), serialize(name));

            if (metaBytes == null) {
                return emptyList();
            }

            MetaMetric meta = deserialize(metaBytes, MetaMetric.class);
            Constructor<Metric<T>> constructor = (Constructor<Metric<T>>) meta.getType().getConstructor(String.class, String.class, Map.class);

            return meta.getTagsById()
                    .entrySet()
                    .stream()
                    .map(entry -> {
                        try {
                            return constructor.newInstance(entry.getKey(), name, entry.getValue());
                        } catch (Exception e) {
                            throw new PugException("Cannot instantiate metric of type " + meta.getType().getTypeName(), e);
                        }
                    })
                    .collect(toList());
        } catch (Exception e) {
            throw new PugException("Cannot select metrics by name " + name, e);
        }
    }

    @Override
    public void insertMetric(Metric<?> metric) {
        try {
            synchronized (metricNameLock.computeIfAbsent(metric.getName(), name -> new Object())) {
                byte[] nameBytes = serialize(metric.getName());
                byte[] metaBytes = db.get(getMetricColumnFamily(), nameBytes);
                MetaMetric meta = metaBytes != null
                                  ? deserialize(metaBytes, MetaMetric.class)
                                  : new MetaMetric(metric.getClass());

                meta.getTagsById().put(metric.getId(), new HashMap<>(metric.getTags()));

                db.put(getMetricColumnFamily(), nameBytes, serialize(meta));

                putMetricIdOnCache(metric);
            }
        } catch (Exception e) {
            throw new PugException("Cannot insert metric " + metric, e);
        }
    }

    boolean putMetricIdOnCache(Metric<?> metric) {
        return getMetricIdsFromCache(metric.getName()).add(metric.getId());
    }

    @SuppressWarnings("unchecked")
    Set<String> getMetricIdsFromCache(String metricName) {
        return metricIdCache.computeIfAbsent(metricName, name -> selectMetricsByName(name).stream().map(Metric::getId).collect(toSet()));
    }
}
