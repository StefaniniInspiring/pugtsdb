package com.inspiring.pugtsdb.repository.h2;

import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.repository.TagRepository;
import com.inspiring.pugtsdb.sql.PugConnection;
import java.util.function.Supplier;

public class H2Repositories implements Repositories {

    private final MetricH2Repository metricRepository;
    private final PointH2Repository pointRepository;
    private final TagH2Repository tagRepository;

    public H2Repositories() {
        this.tagRepository = new TagH2Repository();
        this.pointRepository = new PointH2Repository(tagRepository);
        this.metricRepository = new MetricH2Repository(tagRepository);
    }

    public H2Repositories(Supplier<PugConnection> connectionSupplier) {
        this.tagRepository = new TagH2Repository(connectionSupplier);
        this.pointRepository = new PointH2Repository(connectionSupplier, tagRepository);
        this.metricRepository = new MetricH2Repository(connectionSupplier, tagRepository);
    }

    public void setConnectionSupplier(Supplier<PugConnection> connectionSupplier) {
        metricRepository.setConnectionSupplier(connectionSupplier);
        pointRepository.setConnectionSupplier(connectionSupplier);
        tagRepository.setConnectionSupplier(connectionSupplier);
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
