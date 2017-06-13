package com.inspiring.pugtsdb.repository;

import com.inspiring.pugtsdb.sql.PugConnection;
import java.util.function.Supplier;

public class Repositories {

    private final MetricRepository metricRepository;
    private final DataRepository dataRepository;
    private final TagRepository tagRepository;

    public Repositories(Supplier<PugConnection> connectionSupplier) {
        this.tagRepository = new TagRepository(connectionSupplier);
        this.dataRepository = new DataRepository(connectionSupplier);
        this.metricRepository = new MetricRepository(connectionSupplier, tagRepository);
    }

    public MetricRepository getMetricRepository() {
        return metricRepository;
    }

    public DataRepository getDataRepository() {
        return dataRepository;
    }

    public TagRepository getTagRepository() {
        return tagRepository;
    }
}
