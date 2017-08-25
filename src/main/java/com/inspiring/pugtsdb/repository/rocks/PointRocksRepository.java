package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugNotImplementedException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Granularity;
import java.util.List;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static com.inspiring.pugtsdb.util.Serializer.deserialize;
import static com.inspiring.pugtsdb.util.Serializer.serialize;
import static java.lang.Math.max;
import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;

public class PointRocksRepository extends RocksRepository implements PointRepository {

    private final MetricRocksRepository metricRepository;

    public PointRocksRepository(RocksDB db,
                                ColumnFamilyOptions columnFamilyOptions,
                                Map<String, ColumnFamilyHandle> columnFamilyCache,
                                MetricRocksRepository metricRepository) {
        super(db, columnFamilyOptions, columnFamilyCache);
        this.metricRepository = metricRepository;
    }

    @Override
    public Long selectMaxPointTimestampByNameAndAggregation(String metricName, String aggregation, Granularity granularity) {
        Long timestamp = null;

        for (Integer metricId : metricRepository.getMetricIdsFromCache(metricName)) {
            try (RocksIterator iterator = db.newIterator(getOrCreateValueByTimeColumnFamily(metricId, aggregation, granularity))) {
                iterator.seekToLast();

                if (iterator.isValid()) {
                    timestamp = timestamp != null
                                ? max(timestamp, deserialize(iterator.key(), Long.class))
                                : deserialize(iterator.key(), Long.class);
                }
            }
        }

        return timestamp;
    }

    @Override
    public List<String> selectAggregationNames(String metricName, Granularity granularity) {
        throw new PugNotImplementedException("Selecting aggregation names using RocksDB is not implemented yet");
    }

    @Override
    public <T> MetricPoints<T> selectRawMetricPointsByIdBetweenTimestamp(int metricId, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        throw new PugNotImplementedException("selectRawMetricPointsByIdBetweenTimestamp(int metricId, long fromInclusiveTimestamp, long toExclusiveTimestamp)");
    }

    @Override
    public <T> List<MetricPoints<T>> selectRawMetricsPointsByNameBetweenTimestamp(String metricName, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        return selectMetricsPointsByNameAndAggregationBetweenTimestamp(metricName, null, null, fromInclusiveTimestamp, toExclusiveTimestamp);
    }

    @Override
    public <T> List<MetricPoints<T>> selectRawMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                                         Map<String, String> tags,
                                                                                         long fromInclusiveTimestamp,
                                                                                         long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectMetricPointsByIdAndAggregationBetweenTimestamp(int metricId,
                                                                                    String aggregation,
                                                                                    Granularity granularity,
                                                                                    long fromInclusiveTimestamp,
                                                                                    long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectLastMetricPointsByIdAndAggregation(int metricId, String aggregation, Granularity granularity, int qty) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectMetricPointsByIdBetweenTimestamp(int metricId, Granularity granularity, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectLastMetricPointsById(int metricId, Granularity granularity, int qty) {
        return null;
    }

    @Override
    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationBetweenTimestamp(String metricName,
                                                                                             String aggregation,
                                                                                             Granularity granularity,
                                                                                             long fromInclusiveTimestamp,
                                                                                             long toExclusiveTimestamp) {
        return metricRepository.<T>selectMetricsByName(metricName)
                .stream()
                .map(metric -> {
                    MetricPoints<T> metricPoints = new MetricPoints<>(metric);

                    try (RocksIterator iterator = db.newIterator(getOrCreateValueByTimeColumnFamily(metric.getId(), aggregation, granularity))) {
                        Long timestamp = Long.MAX_VALUE;

                        for (iterator.seek(serialize(fromInclusiveTimestamp)); iterator.isValid() && timestamp < toExclusiveTimestamp; iterator.next()) {
                            timestamp = deserialize(iterator.key(), Long.class);
                            T value = metric.valueFromBytes(iterator.value());
                            metricPoints.put(aggregation, timestamp, value);
                        }
                    }

                    return metricPoints;
                })
                .collect(toList());
    }

    @Override
    public <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndAggregation(String metricName, String aggregation, Granularity granularity, int qty) {
        return null;
    }

    @Override
    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationAndTagsBetweenTimestamp(String metricName,
                                                                                                    String aggregation,
                                                                                                    Granularity granularity,
                                                                                                    Map<String, String> tags,
                                                                                                    long fromInclusiveTimestamp,
                                                                                                    long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndAggregationAndTags(String metricName,
                                                                                        String aggregation,
                                                                                        Granularity granularity,
                                                                                        Map<String, String> tags,
                                                                                        int qty) {
        return null;
    }

    @Override
    public <T> List<MetricPoints<T>> selectMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                                      Granularity granularity,
                                                                                      Map<String, String> tags,
                                                                                      long fromInclusiveTimestamp,
                                                                                      long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndTags(String metricName, Granularity granularity, Map<String, String> tags, int qty) {
        return null;
    }

    @Override
    public <T> void upsertMetricPoint(MetricPoint<T> metricPoint) {
        try {
            Metric<T> metric = metricPoint.getMetric();
            Point<T> point = metricPoint.getPoint();
            byte[] timestampBytes = serialize(point.getTimestamp());
            byte[] valueBytes = metric.valueToBytes(point.getValue());
            ColumnFamilyHandle valueByTimeColumnFamily = getOrCreateValueByTimeColumnFamily(metric.getId(), null, null);

            db.put(valueByTimeColumnFamily, timestampBytes, valueBytes);
        } catch (Exception e) {
            throw new PugException("Cannot upsert metric point " + metricPoint, e);
        }
    }

    @Override
    public <T> void upsertMetricPoints(MetricPoints<T> metricPoints, Granularity granularity) {
        try {
            Metric<T> metric = metricPoints.getMetric();
            Map<String, Map<Long, T>> points = metricPoints.getPoints();

            points.forEach((aggregation, point) -> {
                ColumnFamilyHandle valueByTimeColumnFamily = getOrCreateValueByTimeColumnFamily(metric.getId(), aggregation, granularity);

                point.forEach((timestamp, value) -> {
                    byte[] timestampBytes = serialize(timestamp);
                    byte[] valueBytes = metric.valueToBytes(value);

                    try {
                        db.put(valueByTimeColumnFamily, timestampBytes, valueBytes);
                    } catch (RocksDBException e) {
                        throw new PugException("Cannot upsert value " + value + " on timestamp " + timestamp, e);
                    }
                });
            });
        } catch (Exception e) {
            throw new PugException("Cannot upsert metric points " + metricPoints, e);
        }
    }

    @Override
    public void deleteRawPointsByNameBeforeTime(String metricName, long time) {
        deletePointsByNameAndAggregationBeforeTime(metricName, null, null, time);
    }

    @Override
    public void deletePointsByNameAndAggregationBeforeTime(String metricName, String aggregation, Granularity granularity, long time) {
        try {
            for (Integer metricId : metricRepository.getMetricIdsFromCache(metricName)) {
                ColumnFamilyHandle valueByTimeColumnFamily = getOrCreateValueByTimeColumnFamily(metricId, aggregation, granularity);
                byte[] fromIncludingTime = serialize(0);
                byte[] toExcludeTime = serialize(time - 1);

                db.deleteRange(valueByTimeColumnFamily, fromIncludingTime, toExcludeTime);
            }
        } catch (Exception e) {
            throw new PugException(format("Cannot delete metric {0} points aggregated as {1} in {2} before {3}", metricName, aggregation, granularity, time), e);
        }
    }
}
