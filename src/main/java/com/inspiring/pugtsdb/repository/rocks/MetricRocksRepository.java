package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static com.inspiring.pugtsdb.util.Serializer.deserialize;
import static com.inspiring.pugtsdb.util.Serializer.serialize;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class MetricRocksRepository extends RocksRepository implements MetricRepository {

    private static final String EMPTY_TAG = "";

    private final Map<String, Set<Integer>> metricIdCache = new ConcurrentHashMap<>();

    public MetricRocksRepository() {
        super();
    }

    public MetricRocksRepository(RocksDB db,
                                 ColumnFamilyOptions columnFamilyOptions,
                                 Map<String, ColumnFamilyHandle> columnFamilyCache) {
        super(db, columnFamilyOptions, columnFamilyCache);
    }

    @Override
    public boolean notExistsMetric(Metric<?> metric) {
        return !existsMetric(metric);
    }

    @Override
    public boolean existsMetric(Metric<?> metric) {
        return getMetricIdsFromCache(metric.getName()).contains(metric.getId());
    }

    @Override
    public List<String> selectMetricNames() {
        List<String> names = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator(getOrCreateClassByNameColumnFamily())) {
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
            byte[] classBytes = db.get(getOrCreateClassByNameColumnFamily(), serialize(name));

            if (classBytes == null) {
                return emptyList();
            }

            String metricClass = deserialize(classBytes, String.class);
            Constructor<?> constructor = Class.forName(metricClass).getConstructor(String.class, Map.class);
            Map<Integer, Set<Tag>> tagsById = new HashMap<>();

            try (RocksIterator iterator = db.newIterator(getOrCreateIdsByTagColumnFamily(name))) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    Tag tag = Tag.valueOf(deserialize(iterator.key(), String.class));
                    Set<Integer> ids = deserialize(iterator.value(), HashSet.class);

                    for (Integer id : ids) {
                        Set<Tag> tags = tagsById.computeIfAbsent(id, integer -> new HashSet<>());

                        if (tag != null) {
                            tags.add(tag);
                        }
                    }
                }
            }

            return tagsById.values()
                    .stream()
                    .map(Tag::toMap)
                    .map(tags -> {
                        try {
                            return (Metric<T>) constructor.newInstance(name, tags);
                        } catch (Exception e) {
                            throw new PugException("Cannot instantiate metric of type " + metricClass, e);
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
            db.put(getOrCreateClassByNameColumnFamily(), serialize(metric.getName()), serialize(metric.getClass().getTypeName()));
            ColumnFamilyHandle idsByTagColumnFamily = getOrCreateIdsByTagColumnFamily(metric.getName());

            List<String> tags = metric.getTags().isEmpty()
                                ? singletonList(EMPTY_TAG)
                                : Tag.fromMapToStringList(metric.getTags());

            for (String tag : tags) {
                byte[] tagBytes = serialize(tag);
                Set<Integer> ids = fetchIds(idsByTagColumnFamily, tagBytes);

                if (ids.add(metric.getId())) {
                    db.put(idsByTagColumnFamily, tagBytes, serialize(ids));
                }
            }

            putMetricIdOnCache(metric);
        } catch (Exception e) {
            throw new PugException("Cannot insert metric " + metric, e);
        }
    }

    private Set<Integer> fetchIds(ColumnFamilyHandle idsByTagColumnFamily, byte[] tagBytes) throws RocksDBException {
        byte[] idsBytes = db.get(idsByTagColumnFamily, tagBytes);
        return idsBytes != null
               ? deserialize(idsBytes, HashSet.class)
               : new HashSet<>();
    }

    boolean putMetricIdOnCache(Metric<?> metric) {
        return getMetricIdsFromCache(metric.getName()).add(metric.getId());
    }

    @SuppressWarnings("unchecked")
    Set<Integer> getMetricIdsFromCache(String metricName) {
        return metricIdCache.computeIfAbsent(metricName, name -> {
            Set<Integer> ids = new ConcurrentSkipListSet<>();

            try (RocksIterator iterator = db.newIterator(getOrCreateIdsByTagColumnFamily(name))) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    ids.addAll(deserialize(iterator.value(), ArrayList.class));
                }
            }

            return ids;
        });
    }
}
