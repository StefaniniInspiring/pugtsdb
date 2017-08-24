package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Interval;
import com.inspiring.pugtsdb.time.Retention;
import java.io.Closeable;
import java.util.List;

public interface PugTSDB extends Closeable {

    List<String> selectAggregationNames(String metricName, Granularity granularity);

    List<Metric<Object>> selectMetrics(String name);

    <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, String aggregation, Granularity granularity, Interval interval);

    <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, String aggregation, Granularity granularity, int quantity);

    <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Granularity granularity, Interval interval);

    <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Granularity granularity, int quantity);

    <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Interval interval);

    <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, String aggregation, Granularity granularity, Interval interval, Tag... tags);

    <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, String aggregation, Granularity granularity, int quantity, Tag... tags);

    <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Granularity granularity, Interval interval, Tag... tags);

    <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Granularity granularity, int quantity, Tag... tags);

    <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Interval interval, Tag... tags);

    <T> void upsertMetricPoint(MetricPoint<T> metricPoint);

    <T> void upsertMetricPoint(Metric<T> metric, Point<T> point);

    void registerRollUps(String metricName, Aggregation<?> aggregation, Retention retention, Granularity... granularities);

    void addRollUpListener(String metricName, String aggregationName, Granularity granularity, RollUpListener listener);

    RollUpListener removeRollUpListener(String metricName, String aggregationName, Granularity granularity);
}
