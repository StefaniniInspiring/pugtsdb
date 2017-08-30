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
import java.util.Set;
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

    private static final String LAST_TIMESTAMP = "9999999999999";
    private static final String FIRST_TIMESTAMP = "0000000000000";

    private final MetricRocksRepository metricRepository;

    public PointRocksRepository(MetricRocksRepository metricRepository) {
        this.metricRepository = metricRepository;
    }

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

        for (String metricId : metricRepository.getMetricIdsFromCache(metricName)) {
            try (RocksIterator iterator = db.newIterator(getPointColumnFamily(aggregation, granularity))) {
                iterator.seek(PointId.of(metricId, LAST_TIMESTAMP).toBytes());

                if (iterator.isValid()) {
                    PointId pointId = PointId.from(iterator.key());

                    if (pointId != null && pointId.metricId.equals(metricId)) {
                        timestamp = timestamp != null
                                    ? max(timestamp, pointId.timestamp)
                                    : pointId.timestamp;
                    }
                }
            } catch (Exception e) {
                throw new PugException(format("Cannot select max point timestamp for metric {0}, aggregated as {1} in {2}", metricName, aggregation, granularity), e);
            }
        }

        return timestamp;
    }

    @Override
    public List<String> selectAggregationNames(String metricName, Granularity granularity) {
        Set<String> metricIds = metricRepository.getMetricIdsFromCache(metricName);

        return columnFamilyCache.entrySet()
                .stream()
                .filter(cf -> cf.getKey().startsWith(COLUMN_FAMILY_POINT))
                .filter(cf -> cf.getKey().endsWith(granularity.toString()))
                .filter(cf -> metricIds.stream()
                        .map(id -> {
                            try {
                                byte[] bytes = db.get(cf.getValue(), PointId.of(id, FIRST_TIMESTAMP).toBytes());
                                return bytes != null && PointId.from(bytes).metricId.equals(id);
                            } catch (RocksDBException e) {
                                return false;
                            }
                        })
                        .findFirst()
                        .orElse(false))
                .map(cf -> cf.getKey().substring(cf.getKey().indexOf(SEP) + 1, cf.getKey().lastIndexOf(SEP)))
                .collect(toList());
    }

    @Override
    public <T> MetricPoints<T> selectRawMetricPointsByIdBetweenTimestamp(String metricId, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
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
    public <T> MetricPoints<T> selectMetricPointsByIdAndAggregationBetweenTimestamp(String metricId,
                                                                                    String aggregation,
                                                                                    Granularity granularity,
                                                                                    long fromInclusiveTimestamp,
                                                                                    long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectLastMetricPointsByIdAndAggregation(String metricId, String aggregation, Granularity granularity, int qty) {
        Metric<T> metric = metricRepository.selectMetricById(metricId);

        if (metric == null) {
            return null;
        }

        MetricPoints<T> metricPoints = new MetricPoints<>(metric);

        try (RocksIterator iterator = db.newIterator(getPointColumnFamily(aggregation, granularity))) {
            for (iterator.seek(PointId.of(metricId, LAST_TIMESTAMP).toBytes()); iterator.isValid() && metricPoints.getPoints().size() < qty; iterator.prev()) {
                PointId pointId = PointId.from(iterator.key());

                if (pointId == null || !pointId.metricId.equals(metricId)) {
                    break;
                }

                metricPoints.put(aggregation, pointId.timestamp, iterator.value());
            }
        } catch (Exception e) {
            throw new PugException(format("Cannot select last {3} metric {0} points aggregated as {1} in {2}", metricId, aggregation, granularity, qty), e);
        }

        return metricPoints;
    }

    @Override
    public <T> MetricPoints<T> selectMetricPointsByIdBetweenTimestamp(String metricId, Granularity granularity, long fromInclusiveTimestamp, long toExclusiveTimestamp) {
        return null;
    }

    @Override
    public <T> MetricPoints<T> selectLastMetricPointsById(String metricId, Granularity granularity, int qty) {
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

                    try (RocksIterator iterator = db.newIterator(getPointColumnFamily(aggregation, granularity))) {
                        byte[] fromPointId = PointId.of(metric.getId(), fromInclusiveTimestamp).toBytes();

                        for (iterator.seek(fromPointId); iterator.isValid(); iterator.next()) {
                            PointId pointId = PointId.from(iterator.key());

                            if (pointId == null || !pointId.metricId.equals(metric.getId()) || pointId.timestamp >= toExclusiveTimestamp) {
                                break;
                            }

                            Long timestamp = pointId.timestamp;
                            T value = metric.valueFromBytes(iterator.value());
                            metricPoints.put(aggregation, timestamp, value);
                        }
                    } catch (Exception e) {
                        throw new PugException(format("Cannot select metric {0} points aggregated as {1} in {2} from {3,date} {3,time} to {4,date} {4,time}",
                                                      metricName,
                                                      aggregation,
                                                      granularity,
                                                      fromInclusiveTimestamp,
                                                      toExclusiveTimestamp), e);
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
            byte[] idBytes = PointId.of(metric.getId(), point.getTimestamp()).toBytes();
            byte[] valueBytes = metric.valueToBytes(point.getValue());
            ColumnFamilyHandle pointColumnFamily = getPointColumnFamily(null, null);

            db.put(pointColumnFamily, idBytes, valueBytes);
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
                ColumnFamilyHandle pointColumnFamily = getPointColumnFamily(aggregation, granularity);

                point.forEach((timestamp, value) -> {
                    byte[] idBytes = PointId.of(metric.getId(), timestamp).toBytes();
                    byte[] valueBytes = metric.valueToBytes(value);

                    try {
                        db.put(pointColumnFamily, idBytes, valueBytes);
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
            for (String metricId : metricRepository.getMetricIdsFromCache(metricName)) {
                ColumnFamilyHandle pointColumnFamily = getPointColumnFamily(aggregation, granularity);
                byte[] fromInclusiveId = PointId.of(metricId, FIRST_TIMESTAMP).toBytes();
                byte[] toExclusiveId = PointId.of(metricId, time - 1).toBytes();

                db.deleteRange(pointColumnFamily, fromInclusiveId, toExclusiveId);
            }
        } catch (Exception e) {
            throw new PugException(format("Cannot delete metric {0} points aggregated as {1} in {2} before {3}", metricName, aggregation, granularity, time), e);
        }
    }

    private static class PointId {

        String metricId;
        Long timestamp;

        static PointId of(String metricId, Long timestamp) {
            PointId pointId = new PointId();
            pointId.metricId = metricId;
            pointId.timestamp = timestamp;

            return pointId;
        }

        static PointId of(String metricId, String timestamp) {
            PointId pointId = new PointId();
            pointId.metricId = metricId;
            pointId.timestamp = Long.valueOf(timestamp);

            return pointId;
        }

        static PointId from(byte[] bytes) {
            return bytes == null ? null : from(deserialize(bytes, String.class));
        }

        static PointId from(String string) {
            return of(string.substring(0, Metric.ID_LENGTH), string.substring(Metric.ID_LENGTH));
        }

        @Override
        public String toString() {
            return metricId.concat(timestamp.toString());
        }

        public byte[] toBytes() {
            return serialize(toString());
        }
    }
}
