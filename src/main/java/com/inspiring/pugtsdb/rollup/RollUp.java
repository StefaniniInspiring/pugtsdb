package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpEvent;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.rollup.purge.AggregatedPointPurger;
import com.inspiring.pugtsdb.rollup.purge.RawPointPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.inspiring.pugtsdb.util.Collections.isNotEmpty;
import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class RollUp<T> implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RollUp.class);

    private final String metricName;
    private final Aggregation<T> aggregation;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;
    private final PointRepository pointRepository;
    private final AggregatedPointPurger purger;
    private final RawPointPurger rawPurger;

    private RollUpListener<T> listener = null;
    private Long lastTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<T> aggregation,
                  Granularity sourceGranularity,
                  Granularity targetGranularity,
                  Retention retention,
                  Repositories repositories) {
        this.metricName = metricName;
        this.aggregation = aggregation;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
        this.pointRepository = repositories.getPointRepository();
        this.purger = new AggregatedPointPurger(metricName, aggregation, targetGranularity, retention, pointRepository);
        this.rawPurger = sourceGranularity == null ? new RawPointPurger(pointRepository, Retention.of(2, targetGranularity.getUnit())) : null;

        lastTimestamp = pointRepository.selectMaxPointTimestampByNameAndAggregation(metricName, aggregation.getName(), targetGranularity);

        if (lastTimestamp == null) {
            lastTimestamp = 0L;
        } else {
            lastTimestamp = Instant.ofEpochMilli(lastTimestamp)
                    .atZone(systemDefault())
                    .plus(1, targetGranularity.getUnit())
                    .toEpochSecond() * 1000;
        }
    }

    public String getMetricName() {
        return metricName;
    }

    public Aggregation<T> getAggregation() {
        return aggregation;
    }

    public Granularity getSourceGranularity() {
        return sourceGranularity;
    }

    public Granularity getTargetGranularity() {
        return targetGranularity;
    }

    public RollUpListener getListener() {
        return listener;
    }

    public void setListener(RollUpListener<T> listener) {
        this.listener = listener;
    }

    @Override
    public void run() {
        long nextTimestamp = truncate(Instant.now(), targetGranularity.getUnit());
        long runTimestamp = currentTimeMillis();
        Data data;

        log.trace("Roll up per {} running {}", targetGranularity, this);

        try {
            long fetchStartTime = currentTimeMillis();
            data = fetchSourceData(nextTimestamp);
            log.trace("Roll up per {} fetched {}: Took={}", targetGranularity, this, currentTimeMillis() - fetchStartTime);

            long aggregateStartTime = currentTimeMillis();
            aggregateData(data);
            log.trace("Roll up per {} aggregated {}: Took={}", targetGranularity, this, currentTimeMillis() - aggregateStartTime);

            long saveStartTime = currentTimeMillis();
            saveData(data);
            log.trace("Roll up per {} saved {}: Took={}", targetGranularity, this, currentTimeMillis() - saveStartTime);
        } catch (Exception e) {
            log.error("Cannot perform {}", this, e);
            return;
        } finally {
            lastTimestamp = nextTimestamp;
        }

        purger.run();

        if (rawPurger != null) {
            rawPurger.run();
        }

        if (listener != null && isNotEmpty(data.metricsPoints)) {
            runAsync(() -> listener.onRollUp(new RollUpEvent<>(metricName, aggregation.getName(), sourceGranularity, targetGranularity, data.metricsPoints)));
        }

        log.trace("Roll up per {} done {}: Took={}ms", targetGranularity, this, currentTimeMillis() - runTimestamp);
    }

    private Data fetchSourceData(long nextTimestamp) {
        Data data = new Data();

        try {
            if (data.isRaw()) {
                data.metricsPoints = pointRepository.selectRawMetricsPointsByNameBetweenTimestamp(metricName, lastTimestamp, nextTimestamp);
                data.sourceAggregation = null;
            } else {
                data.metricsPoints = pointRepository.selectMetricsPointsByNameAndAggregationBetweenTimestamp(metricName,
                                                                                                             aggregation.getName(),
                                                                                                             sourceGranularity,
                                                                                                             lastTimestamp,
                                                                                                             nextTimestamp);
                data.sourceAggregation = aggregation.getName();
            }
        } finally {
            pointRepository.getConnection().close();
        }

        return data;
    }

    private void aggregateData(Data data) {
        for (MetricPoints<T> metricPoints : data.metricsPoints) {
            metricPoints.getPoints()
                    .computeIfPresent(data.sourceAggregation,
                                      (aggregation, points) -> points.isEmpty()
                                                               ? null
                                                               : points.entrySet()
                                                                       .stream()
                                                                       .collect(groupingBy(point -> truncate(point.getKey(), targetGranularity.getUnit()),
                                                                                           mapping(point -> point.getValue(), toList())))
                                                                       .entrySet()
                                                                       .stream()
                                                                       .collect(() -> new TreeMap<>(),
                                                                                (map, entry) -> map.put(entry.getKey(), this.aggregation.aggregate(entry.getValue())),
                                                                                (map1, map2) -> map1.putAll(map2)));

            if (data.isRaw()) {
                Map<Long, T> points = metricPoints.getPoints().remove(null);

                if (isNotEmpty(points)) {
                    metricPoints.getPoints().put(aggregation.getName(), points);
                }
            }
        }
    }

    private void saveData(Data data) {
        try {
            data.metricsPoints
                    .forEach(metricPoints -> pointRepository.upsertMetricPoints(metricPoints, targetGranularity));
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            pointRepository.getConnection().rollback();
            throw e;
        } finally {
            pointRepository.getConnection().close();
        }
    }

    @Override
    public String toString() {
        return "RollUp{" +
                "metricName='" + metricName + '\'' +
                ", aggregation=" + aggregation.getName() +
                ", sourceGranularity=" + sourceGranularity +
                ", targetGranularity=" + targetGranularity +
                '}';
    }

    private class Data {

        List<MetricPoints<T>> metricsPoints;
        String sourceAggregation;

        boolean isRaw() {
            return sourceGranularity == null;
        }
    }
}
