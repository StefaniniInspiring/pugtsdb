package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.time.Granularity;
import java.util.List;
import java.util.Map;

public interface PointRepository extends Repository {

    Long selectMaxPointTimestampByNameAndAggregation(String metricName, String aggregation, Granularity granularity);

    List<String> selectAggregationNames(String metricName, Granularity granularity);

    <T> MetricPoints<T> selectRawMetricPointsByIdBetweenTimestamp(String metricId, long fromInclusiveTimestamp, long toExclusiveTimestamp);

    <T> List<MetricPoints<T>> selectRawMetricsPointsByNameBetweenTimestamp(String metricName, long fromInclusiveTimestamp, long toExclusiveTimestamp);

    <T> List<MetricPoints<T>> selectRawMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                                  Map<String, String> tags,
                                                                                  long fromInclusiveTimestamp,
                                                                                  long toExclusiveTimestamp);

    <T> MetricPoints<T> selectMetricPointsByIdAndAggregationBetweenTimestamp(String metricId,
                                                                             String aggregation,
                                                                             Granularity granularity,
                                                                             long fromInclusiveTimestamp,
                                                                             long toExclusiveTimestamp);

    <T> MetricPoints<T> selectLastMetricPointsByIdAndAggregation(String metricId,
                                                                 String aggregation,
                                                                 Granularity granularity,
                                                                 int qty);

    <T> MetricPoints<T> selectMetricPointsByIdBetweenTimestamp(String metricId,
                                                               Granularity granularity,
                                                               long fromInclusiveTimestamp,
                                                               long toExclusiveTimestamp);

    <T> MetricPoints<T> selectLastMetricPointsById(String metricId,
                                                   Granularity granularity,
                                                   int qty);

    <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationBetweenTimestamp(String metricName,
                                                                                      String aggregation,
                                                                                      Granularity granularity,
                                                                                      long fromInclusiveTimestamp,
                                                                                      long toExclusiveTimestamp);

    <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndAggregation(String metricName,
                                                                          String aggregation,
                                                                          Granularity granularity,
                                                                          int qty);

    <T> List<MetricPoints<T>> selectMetricsPointsByNameAndAggregationAndTagsBetweenTimestamp(String metricName,
                                                                                             String aggregation,
                                                                                             Granularity granularity,
                                                                                             Map<String, String> tags,
                                                                                             long fromInclusiveTimestamp,
                                                                                             long toExclusiveTimestamp);

    <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndAggregationAndTags(String metricName,
                                                                                 String aggregation,
                                                                                 Granularity granularity,
                                                                                 Map<String, String> tags,
                                                                                 int qty);

    <T> List<MetricPoints<T>> selectMetricsPointsByNameAndTagsBetweenTimestamp(String metricName,
                                                                               Granularity granularity,
                                                                               Map<String, String> tags,
                                                                               long fromInclusiveTimestamp,
                                                                               long toExclusiveTimestamp);

    <T> List<MetricPoints<T>> selectLastMetricsPointsByNameAndTags(String metricName,
                                                                   Granularity granularity,
                                                                   Map<String, String> tags,
                                                                   int qty);

    <T> void upsertMetricPoint(MetricPoint<T> metricPoint);

    <T> void upsertMetricPoints(MetricPoints<T> metricPoints, Granularity granularity);

    void deleteRawPointsByNameBeforeTime(String metricName, long time);

    void deletePointsByNameAndAggregationBeforeTime(String metricName, String aggregation, Granularity granularity, long time);
}
