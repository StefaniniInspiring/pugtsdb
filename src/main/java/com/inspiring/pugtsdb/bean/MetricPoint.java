package com.inspiring.pugtsdb.bean;

import com.inspiring.pugtsdb.metric.Metric;

public class MetricPoint<T> {

    private final Metric<T> metric;
    private final Point<T> point;

    public MetricPoint(Metric<T> metric, Point<T> point) {
        this.metric = metric;
        this.point = point;
    }

    public static <V> MetricPoint<V> of(Metric<V> metric, Point<V> point) {
        return new MetricPoint<>(metric, point);
    }

    public Metric<T> getMetric() {
        return metric;
    }

    public Point<T> getPoint() {
        return point;
    }

    @Override
    public String toString() {
        return "MetricPoint{" +
                "metric=" + metric +
                ", point=" + point +
                '}';
    }
}
