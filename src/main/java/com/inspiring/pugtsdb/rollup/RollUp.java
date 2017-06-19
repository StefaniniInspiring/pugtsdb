package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.AggregatedPointPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.inspiring.pugtsdb.util.Collections.isNotEmpty;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.util.stream.Collectors.toMap;

public class RollUp<T> implements Runnable {

    private final String metricName;
    private final Aggregation<T> aggregation;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;
    private final Repositories repositories;
    private final AggregatedPointPurger purger;

    private Long lastTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<T> aggregation,
                  Granularity sourceGranularity, Granularity targetGranularity,
                  Retention retention,
                  Repositories repositories) {
        this.metricName = metricName;
        this.aggregation = aggregation;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
        this.repositories = repositories;
        this.purger = new AggregatedPointPurger(metricName, aggregation, targetGranularity, retention, repositories.getPointRepository());

        lastTimestamp = repositories.getPointRepository().selectMaxPointTimestampByNameAndAggregation(metricName, aggregation.getName(), targetGranularity);

        if (lastTimestamp == null) {
            lastTimestamp = 0L;
        } else {
            lastTimestamp = Instant.ofEpochMilli(lastTimestamp)
                    .atZone(systemDefault())
                    .plus(1, targetGranularity.getUnit())
                    .toEpochSecond() * 1000;
        }
    }

    public Granularity getTargetGranularity() {
        return targetGranularity;
    }

    @Override
    public void run() {
        long nextTimestamp = truncateTimestamp(currentTimeMillis());

        Data data = fetchSourceData(nextTimestamp);
        aggregateData(data);
        saveData(data);

        lastTimestamp = nextTimestamp;

        purger.run();
    }

    private Data fetchSourceData(long nextTimestamp) {
        Data data = new Data();
        MetricRepository metricRepository = repositories.getMetricRepository();

        if (data.isRaw()) {
            data.pointsList = metricRepository.selectRawMetricPointsByNameBetweenTimestamp(metricName, lastTimestamp, nextTimestamp);
            data.sourceAggregation = null;
        } else {
            data.pointsList = metricRepository.selectMetricPointsByNameAndAggregationBetweenTimestamp(metricName,
                                                                                                      aggregation.getName(),
                                                                                                      sourceGranularity,
                                                                                                      lastTimestamp,
                                                                                                      nextTimestamp);
            data.sourceAggregation = aggregation.getName();
        }

        return data;
    }

    private void aggregateData(Data data) {
        for (MetricPoints<T> points : data.pointsList) {
            points.getValues()
                    .computeIfPresent(data.sourceAggregation, (key, values) -> values.entrySet()
                            .stream()
                            .collect(toMap(point -> truncateTimestamp(point.getKey()),
                                           point -> point.getValue(),
                                           (value1, value2) -> aggregation.aggregate(value1, value2),
                                           TreeMap::new)));

            if (data.isRaw()) {
                Map<Long, T> values = points.getValues().remove(null);

                if (isNotEmpty(values)) {
                    points.getValues().put(aggregation.getName(), values);
                }
            }
        }
    }

    private void saveData(Data data) {
        data.pointsList.forEach(points -> repositories.getPointRepository().upsertMetricPoints(points, targetGranularity));
    }

    private long truncateTimestamp(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .truncatedTo(targetGranularity.getUnit())
                .toEpochMilli();
    }

    private class Data {

        List<MetricPoints<T>> pointsList;
        String sourceAggregation;

        boolean isRaw() {
            return sourceGranularity == null;
        }
    }
}
