package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.repository.PointRepository;
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
import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.time.ZoneId.systemDefault;
import static java.util.stream.Collectors.toMap;

public class RollUp<T> implements Runnable {

    private final String metricName;
    private final Aggregation<T> aggregation;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;
    private final PointRepository pointRepository;
    private final AggregatedPointPurger purger;

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

    public Granularity getTargetGranularity() {
        return targetGranularity;
    }

    @Override
    public void run() {
        long nextTimestamp = truncate(Instant.now(), targetGranularity.getUnit());

        Data data = fetchSourceData(nextTimestamp);
        aggregateData(data);
        saveData(data);

        lastTimestamp = nextTimestamp;

        purger.run();
    }

    private Data fetchSourceData(long nextTimestamp) {
        Data data = new Data();

        if (data.isRaw()) {
            data.pointsList = pointRepository.selectRawMetricPointsByNameBetweenTimestamp(metricName, lastTimestamp, nextTimestamp);
            data.sourceAggregation = null;
        } else {
            data.pointsList = pointRepository.selectMetricPointsByNameAndAggregationBetweenTimestamp(metricName,
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
                            .collect(toMap(point -> truncate(point.getKey(), targetGranularity.getUnit()),
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
        data.pointsList.forEach(points -> pointRepository.upsertMetricPoints(points, targetGranularity));
    }

    private class Data {

        List<MetricPoints<T>> pointsList;
        String sourceAggregation;

        boolean isRaw() {
            return sourceGranularity == null;
        }
    }
}
