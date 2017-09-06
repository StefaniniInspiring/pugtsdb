package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpEvent;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.time.Granularity;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private RollUpListener<T> listener = null;
    private Long lastTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<T> aggregation,
                  Granularity sourceGranularity,
                  Granularity targetGranularity,
                  Repositories repositories) {
        this.metricName = metricName;
        this.aggregation = aggregation;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
        this.pointRepository = repositories.getPointRepository();

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
        long runStartTime = currentTimeMillis();
        Data data;

        log.trace("{} is rolling up...", this);

        try {
            data = fetchSourceData(nextTimestamp);
            aggregateData(data);
            saveData(data);
        } catch (Exception e) {
            log.error("Cannot perform {}", this, e);
            return;
        } finally {
            lastTimestamp = nextTimestamp;
        }

        notifyListener(data);

        log.debug("{} rolled up: Took={}ms", this, currentTimeMillis() - runStartTime);
    }

    private Data fetchSourceData(long nextTimestamp) {
        long fetchStartTime = currentTimeMillis();
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

        log.trace("{} fetched: Took={}ms", this, currentTimeMillis() - fetchStartTime);

        return data;
    }

    private void aggregateData(Data data) {
        long aggregateStartTime = currentTimeMillis();

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

        log.trace("{} aggregated: Took={}ms", this, currentTimeMillis() - aggregateStartTime);
    }

    private void saveData(Data data) {
        long saveStartTime = currentTimeMillis();

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

        log.trace("{} saved: Took={}ms", this, currentTimeMillis() - saveStartTime);
    }

    private void notifyListener(Data data) {
        if (listener != null && data.isNotEmpty()) {
            long notifyStartTime = currentTimeMillis();
            runAsync(() -> listener.onRollUp(new RollUpEvent<>(metricName, aggregation.getName(), sourceGranularity, targetGranularity, data.metricsPoints)));
            log.trace("{} notified listener: Took={}", this, currentTimeMillis() - notifyStartTime);
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

        boolean isNotEmpty() {
            return metricsPoints != null && metricsPoints.stream()
                    .map(MetricPoints::getPoints)
                    .anyMatch(points -> points.values()
                            .stream()
                            .filter(Objects::nonNull)
                            .anyMatch(point -> !point.isEmpty()));
        }
    }
}
