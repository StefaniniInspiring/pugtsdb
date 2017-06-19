package com.inspiring.pugtsdb.bean;

import com.inspiring.pugtsdb.metric.Metric;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

public class MetricPoints<T> {

    private final Metric<T> metric;
    private final Map<String, Map<Long, T>> values = new TreeMap<>(nullsFirst(naturalOrder()));

    public MetricPoints(Metric<T> metric) {
        this.metric = metric;
    }

    public Metric<T> getMetric() {
        return metric;
    }

    public Map<String, Map<Long, T>> getValues() {
        return values;
    }

    public void put(String aggregation, long timestamp, byte[] bytes) {
        put(aggregation, timestamp, metric.valueFromBytes(bytes));
    }

    public void put(String aggregation, long timestamp, T value) {
        values.computeIfAbsent(aggregation, s -> new TreeMap<>()).put(timestamp, value);
    }

    @Override
    public String toString() {
        return "MetricPoints{" +
                "metric=" + metric +
                ", values=" + values +
                '}';
    }
}
