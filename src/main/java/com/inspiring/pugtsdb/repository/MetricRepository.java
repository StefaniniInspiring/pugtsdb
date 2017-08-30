package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.metric.Metric;
import java.util.List;

public interface MetricRepository extends Repository {

    default boolean notExistsMetric(Metric<?> metric) {
        return !existsMetric(metric);
    }

    boolean existsMetric(Metric<?> metric);

    List<String> selectMetricNames();

    <T> List<Metric<T>> selectMetricsByName(String name);

    void insertMetric(Metric<?> metric);
}
