package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.metric.Metric;
import java.util.List;

public interface MetricRepository extends Repository {

    boolean notExistsMetric(Metric<?> metric);

    boolean existsMetric(Metric<?> metric);

    List<String> selectMetricNames();

    List<Metric<Object>> selectMetricsByName(String name);

    void insertMetric(Metric<?> metric);
}
