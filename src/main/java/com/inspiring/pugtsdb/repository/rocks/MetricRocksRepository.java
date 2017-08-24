package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static com.inspiring.pugtsdb.util.Serializer.deserialize;
import static com.inspiring.pugtsdb.util.Serializer.serialize;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class MetricRocksRepository extends RocksRepository implements MetricRepository {

    @Override
    public boolean notExistsMetric(Metric<?> metric) {
        return !existsMetric(metric);
    }

    @Override
    public boolean existsMetric(Metric<?> metric) {
        ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get("ids_by_tag:" + metric.getName());

        if (columnFamilyHandle == null) {
            return false;
        }

        try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                List<Integer> ids = deserialize(iterator.value(), List.class);

                if (ids.contains(metric.getId())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public List<String> selectMetricNames() {
        List<String> names = new ArrayList<>();
        ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get("metric_class");

        try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                names.add(deserialize(iterator.key(), String.class));
            }
        }

        return names;
    }

    @Override
    public List<Metric<Object>> selectMetricsByName(String name) {
        try {
            byte[] bytes = db.get(columnFamilyHandleMap.get("metric_class"), serialize(name));

            if (bytes == null) {
                return emptyList();
            }

            String metricClass = deserialize(bytes, String.class);
            Constructor<?> constructor = Class.forName(metricClass).getConstructor(String.class, Map.class);
            ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get("ids_by_tag:" + name);

            if (columnFamilyHandle == null) {
                return singletonList((Metric<Object>) constructor.newInstance(name, emptyMap()));
            }

            Map<Integer, Set<Tag>> tagsById = new HashMap<>();

            try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    Tag tag = Tag.valueOf(deserialize(iterator.key(), String.class));
                    List<Integer> ids = deserialize(iterator.value(), List.class);

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
                            return (Metric<Object>) constructor.newInstance(name, tags);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertMetric(Metric<?> metric) {
        try {
            db.put(columnFamilyHandleMap.get("metric_class"), serialize(metric.getName()), serialize(metric.getClass().getTypeName()));
            String columnFamilyName = "ids_by_tag:" + metric.getName();
            ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(columnFamilyName);

            if (columnFamilyHandle == null) {
                if (metric.getTags().isEmpty()) {
                    db.put(createColumnFamily(columnFamilyName), serialize(null), serialize(singletonList(metric.getId())));
                } else {
                    for (Entry<String, String> tag : metric.getTags().entrySet()) {
                        db.put(createColumnFamily(columnFamilyName), serialize(tag.getKey() + "=" + tag.getValue()), serialize(singletonList(metric.getId())));
                    }
                }
            } else {
                if (metric.getTags().isEmpty()) {
                    byte[] key = serialize(null);
                    List<Integer> ids = deserialize(db.get(columnFamilyHandle, key), List.class);
                    ids.add(metric.getId());
                    db.put(columnFamilyHandle, key, serialize(ids));
                } else {
                    for (Entry<String, String> tag : metric.getTags().entrySet()) {
                        byte[] key = serialize(tag.getKey() + "=" + tag.getValue());
                        List<Integer> ids = deserialize(db.get(columnFamilyHandle, key), List.class);
                        ids.add(metric.getId());
                        db.put(columnFamilyHandle, key, serialize(ids));
                    }
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
