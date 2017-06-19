package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.sql.PugConnection;
import java.util.function.Supplier;

public class Repositories {

    private final MetricRepository metricRepository;
    private final PointRepository pointRepository;
    private final TagRepository tagRepository;

    public Repositories(Supplier<PugConnection> connectionSupplier) {
        this.tagRepository = new TagRepository(connectionSupplier);
        this.pointRepository = new PointRepository(connectionSupplier);
        this.metricRepository = new MetricRepository(connectionSupplier, tagRepository);
    }

    public MetricRepository getMetricRepository() {
        return metricRepository;
    }

    public PointRepository getPointRepository() {
        return pointRepository;
    }

    public TagRepository getTagRepository() {
        return tagRepository;
    }
}
