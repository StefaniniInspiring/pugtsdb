package com.inspiring.pugtsdb.repository.rocks;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugNotImplementedException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.rocks.bean.PointId;
import com.inspiring.pugtsdb.time.Granularity;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import static java.lang.Math.max;
import static java.text.MessageFormat.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public class PointRocksRepository extends RocksRepository implements PointRepository {

    private static final Long LAST_TIMESTAMP = 9999999999999L;
    private static final Long FIRST_TIMESTAMP = 0L;

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

        try (ReadOptions options = newFastReadOptions().setIgnoreRangeDeletions(false);
             RocksIterator iterator = db.newIterator(fromPointColumnFamily(aggregation, granularity), options)) {
            for (String metricId : metricRepository.getMetricIdsFromCache(metricName)) {
                iterator.seekForPrev(PointId.of(metricId, LAST_TIMESTAMP).toBytes());

                if (iterator.isValid()) {
                    PointId pointId = PointId.from(iterator.key());

                    if (pointId.metricId.equals(metricId)) {
                        timestamp = timestamp != null
                                    ? max(timestamp, pointId.timestamp)
                                    : pointId.timestamp;
                    }
                }
            }
        } catch (Exception e) {
            throw new PugException(format("Cannot select max point timestamp for metric {0}, aggregated as {1} in {2}", metricName, aggregation, granularity), e);
        }

        return timestamp;
    }

    @Override
    public List<String> selectAggregationNames(String metricName, Granularity granularity) {
        Set<String> metricIds = metricRepository.getMetricIdsFromCache(metricName);

        return columnFamilyCache.entrySet()
                .stream()
                .filter(cf -> cf.getKey().startsWith(POINT_COLUMN_FAMILY))
                .filter(cf -> cf.getKey().endsWith(granularity.toString()))
                .filter(cf -> {
                    try (ReadOptions options = newFastReadOptions().setIgnoreRangeDeletions(false);
                         RocksIterator iterator = db.newIterator(cf.getValue(), options)) {
                        for (String metricId : metricIds) {
                            iterator.seek(PointId.of(metricId, FIRST_TIMESTAMP).toBytes());

                            if (iterator.isValid() && PointId.from(iterator.key()).metricId.equals(metricId)) {
                                return true;
                            }
                        }

                        return false;
                    }
                })
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
        Map<String, Map<Long, T>> points = metricPoints.getPoints();

        try (ReadOptions options = newFastReadOptions();
             RocksIterator iterator = db.newIterator(fromPointColumnFamily(aggregation, granularity), options)) {
            for (iterator.seekForPrev(PointId.of(metricId, LAST_TIMESTAMP).toBytes());
                 iterator.isValid() && points.getOrDefault(aggregation, emptyMap()).size() < qty;
                 iterator.prev()) {
                PointId pointId = PointId.from(iterator.key());

                if (pointId.metricId.equals(metricId)) {
                    metricPoints.put(aggregation, pointId.timestamp, iterator.value());
                } else {
                    break;
                }
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

                    try (ReadOptions options = newFastReadOptions();
                         RocksIterator iterator = db.newIterator(fromPointColumnFamily(aggregation, granularity), options)) {
                        byte[] fromPointId = PointId.of(metric.getId(), fromInclusiveTimestamp).toBytes();

                        for (iterator.seek(fromPointId); iterator.isValid(); iterator.next()) {
                            PointId pointId = PointId.from(iterator.key());

                            if (!pointId.metricId.equals(metric.getId()) || pointId.timestamp >= toExclusiveTimestamp) {
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
        try (WriteOptions options = new WriteOptions().setSync(false)) {
            Metric<T> metric = metricPoint.getMetric();
            Point<T> point = metricPoint.getPoint();
            byte[] idBytes = PointId.of(metric.getId(), point.getTimestamp()).toBytes();
            byte[] valueBytes = metric.valueToBytes(point.getValue());

            db.put(intoPointColumnFamily(null, null), options, idBytes, valueBytes);
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
                ColumnFamilyHandle pointColumnFamily = intoPointColumnFamily(aggregation, granularity);

                point.forEach((timestamp, value) -> {
                    byte[] idBytes = PointId.of(metric.getId(), timestamp).toBytes();
                    byte[] valueBytes = metric.valueToBytes(value);

                    try (WriteOptions options = new WriteOptions().setSync(false)) {
                        db.put(pointColumnFamily, options, idBytes, valueBytes);
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
        ColumnFamilyHandle fromPointColumnFamily = fromPointColumnFamily(aggregation, granularity);

        try (ReadOptions options = newFastReadOptions().setIgnoreRangeDeletions(false);
             RocksIterator iterator = db.newIterator(fromPointColumnFamily, options)) {
            for (String metricId : metricRepository.getMetricIdsFromCache(metricName)) {
                byte[] fromInclusiveId = null;
                byte[] toExclusiveId = null;

                iterator.seek(PointId.of(metricId, FIRST_TIMESTAMP).toBytes());

                if (iterator.isValid()) {
                    PointId pointId = PointId.from(iterator.key());

                    if (pointId.metricId.equals(metricId) && pointId.timestamp < time) {
                        fromInclusiveId = iterator.key();
                    }
                }

                if (fromInclusiveId == null) {
                    continue;
                }

                iterator.seekForPrev(PointId.of(metricId, time).toBytes());

                if (iterator.isValid()) {
                    PointId pointId = PointId.from(iterator.key());

                    if (pointId.metricId.equals(metricId) && pointId.timestamp < time) {
                        toExclusiveId = iterator.key();
                    }
                }

                try (WriteOptions writeOptions = new WriteOptions().setNoSlowdown(true).setSync(false)) {
                    if (toExclusiveId == null || Arrays.equals(fromInclusiveId, toExclusiveId)) {
                        db.delete(fromPointColumnFamily, writeOptions, fromInclusiveId);
                    } else {
                        db.deleteRange(fromPointColumnFamily, writeOptions, fromInclusiveId, toExclusiveId);
                    }
                }
            }
        } catch (Exception e) {
            throw new PugException(format("Cannot delete metric {0} points aggregated as {1} in {2} before {3}", metricName, aggregation, granularity, time), e);
        }
    }
}
