package com.inspiring.pugtsdb.repository;

public interface Repositories {

    MetricRepository getMetricRepository();

    PointRepository getPointRepository();

    TagRepository getTagRepository();
}
