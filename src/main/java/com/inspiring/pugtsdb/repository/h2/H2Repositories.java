package com.inspiring.pugtsdb.repository.h2;

import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.repository.TagRepository;
import com.inspiring.pugtsdb.sql.PugConnection;
import java.util.function.Supplier;

public class H2Repositories implements Repositories {

    private final MetricRepository metricRepository;
    private final PointRepository pointRepository;
    private final TagRepository tagRepository;

    public H2Repositories(Supplier<PugConnection> connectionSupplier) {
        this.tagRepository = new TagH2Repository(connectionSupplier);
        this.pointRepository = new PointH2Repository(connectionSupplier, tagRepository);
        this.metricRepository = new MetricH2Repository(connectionSupplier, tagRepository);
    }

    @Override
    public MetricRepository getMetricRepository() {
        return metricRepository;
    }

    @Override
    public PointRepository getPointRepository() {
        return pointRepository;
    }

    @Override
    public TagRepository getTagRepository() {
        return tagRepository;
    }
}
