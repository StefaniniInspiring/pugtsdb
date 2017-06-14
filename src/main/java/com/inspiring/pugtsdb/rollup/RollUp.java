package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.AggregatedDataPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TreeMap;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toMap;

public class RollUp implements Runnable {

    private final String metricName;
    private final Aggregation<Object> aggregation;
    private final Granularity granularity;
    private final Repositories repositories;
    private final AggregatedDataPurger purger;

    private Long lastTimestamp = null;
    private Long nextTimestamp = null;

    public RollUp(String metricName,
                  Aggregation<Object> aggregation,
                  Granularity granularity,
                  Retention retention,
                  Repositories repositories) {
        this.metricName = metricName;
        this.aggregation = aggregation;
        this.granularity = granularity;
        this.repositories = repositories;
        this.purger = new AggregatedDataPurger(metricName, aggregation, granularity, retention, repositories.getDataRepository());

        lastTimestamp = repositories.getDataRepository().selectMaxAggregatedDataTimestamp(metricName, aggregation.getName(), granularity);

        if (lastTimestamp == null) {
            lastTimestamp = 0L;
        } else {
            lastTimestamp = Instant.ofEpochMilli(lastTimestamp).atZone(ZoneId.systemDefault()).plus(1, granularity.getUnit()).toEpochSecond() * 1000;
        }
    }

    @Override
    public void run() {
        nextTimestamp = truncateTimestamp(currentTimeMillis());

        repositories.getMetricRepository()
                .selectMetricPointsByNameAndTimestamp(metricName, aggregation.getName(), granularity, lastTimestamp, nextTimestamp)
                .forEach(points -> points.getValues()
                        .computeIfPresent(aggregation.getName(), (key, values) -> values.entrySet()
                                .stream()
                                .collect(toMap(point -> truncateTimestamp(point.getKey()),
                                               point -> point.getValue(),
                                               (value1, value2) -> aggregation.aggregate(value1, value2),
                                               TreeMap::new))));

        lastTimestamp = nextTimestamp;

        purger.run();
    }

    private long truncateTimestamp(long timestamp) {
        return Instant.ofEpochMilli(timestamp).truncatedTo(granularity.getUnit()).toEpochMilli();
    }
}
