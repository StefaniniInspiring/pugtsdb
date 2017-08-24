package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.metric.Metric;
import java.util.List;

public interface MetricRepository extends Repository {

    boolean notExistsMetric(Integer id);

    boolean existsMetric(Integer id);

    List<String> selectMetricNames();

    List<Metric<Object>> selectMetricsByName(String name);

    void insertMetric(Metric<?> metric);
}
