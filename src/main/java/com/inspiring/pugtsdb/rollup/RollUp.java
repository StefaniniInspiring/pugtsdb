package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.AggregatedDataPurger;
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

public class RollUp implements Runnable {

    private final String metricName;
    private final Aggregation<Object> aggregation;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;
    private final Repositories repositories;
    private final AggregatedDataPurger purger;

    private Long lastTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<Object> aggregation,
                  Granularity sourceGranularity, Granularity targetGranularity,
                  Retention retention,
                  Repositories repositories) {
        this.metricName = metricName;
        this.aggregation = aggregation;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
        this.repositories = repositories;
        this.purger = new AggregatedDataPurger(metricName, aggregation, targetGranularity, retention, repositories.getDataRepository());

        lastTimestamp = repositories.getDataRepository().selectMaxAggregatedDataTimestamp(metricName, aggregation.getName(), targetGranularity);

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
        List<MetricPoints> metricPoints;
        String sourceAggregation;
        boolean isSourceRawData = sourceGranularity == null;

        if (isSourceRawData) {
            metricPoints = repositories.getMetricRepository().selectRawMetricPointsByNameBetweenTimestamp(metricName, lastTimestamp, nextTimestamp);
            sourceAggregation = null;
        } else {
            metricPoints = repositories.getMetricRepository().selectMetricPointsByNameAndAggregationBetweenTimestamp(metricName,
                                                                                                                     aggregation.getName(),
                                                                                                                     sourceGranularity,
                                                                                                                     lastTimestamp,
                                                                                                                     nextTimestamp);
            sourceAggregation = aggregation.getName();
        }

        for (MetricPoints points : metricPoints) {
            points.getValues()
                    .computeIfPresent(sourceAggregation, (key, values) -> values.entrySet()
                            .stream()
                            .collect(toMap(point -> truncateTimestamp(point.getKey()),
                                           point -> point.getValue(),
                                           (value1, value2) -> aggregation.aggregate(value1, value2),
                                           TreeMap::new)));
        }

        if (isSourceRawData) {
            for (MetricPoints points : metricPoints) {
                Map<Long, Object> values = points.getValues().remove(sourceAggregation);

                if (isNotEmpty(values)) {
                    points.getValues().put(aggregation.getName(), values);
                }
            }
        }

        //TODO insert points

        lastTimestamp = nextTimestamp;

        purger.run();
    }

    private long truncateTimestamp(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .truncatedTo(targetGranularity.getUnit())
                .toEpochMilli();
    }
}
