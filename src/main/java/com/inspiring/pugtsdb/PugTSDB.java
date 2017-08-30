package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.bean.MetricPoint;
import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.bean.Point;
import com.inspiring.pugtsdb.bean.Tag;
import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
import com.inspiring.pugtsdb.rollup.schedule.RollUpScheduler;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Interval;
import com.inspiring.pugtsdb.time.Retention;
import java.util.List;

public abstract class PugTSDB implements AutoCloseable {

    protected final Repositories repositories;
    protected final RollUpScheduler rollUpScheduler;

    protected PugTSDB(Repositories repositories) {
        this.repositories = repositories;
        this.rollUpScheduler = new RollUpScheduler(repositories);
    }

    public List<String> selectAggregationNames(String metricName, Granularity granularity) {
        try {
            return repositories.getPointRepository().selectAggregationNames(metricName, granularity);
        } finally {
            closeConnection();
        }
    }

    public List<Metric<Object>> selectMetrics(String name) {
        try {
            return repositories.getMetricRepository().selectMetricsByName(name);
        } finally {
            closeConnection();
        }
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, String aggregation, Granularity granularity, Interval interval) {
        try {
            return repositories.getPointRepository()
                    .selectMetricPointsByIdAndAggregationBetweenTimestamp(metric.getId(), aggregation, granularity, interval.getFromTime(), interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, String aggregation, Granularity granularity, int quantity) {
        try {
            return repositories.getPointRepository()
                    .selectLastMetricPointsByIdAndAggregation(metric.getId(), aggregation, granularity, quantity);
        } finally {
            closeConnection();
        }
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Granularity granularity, Interval interval) {
        try {
            return repositories.getPointRepository()
                    .selectMetricPointsByIdBetweenTimestamp(metric.getId(), granularity, interval.getFromTime(), interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Granularity granularity, int quantity) {
        try {
            return repositories.getPointRepository()
                    .selectLastMetricPointsById(metric.getId(), granularity, quantity);
        } finally {
            closeConnection();
        }
    }

    public <T> MetricPoints<T> selectMetricPoints(Metric<T> metric, Interval interval) {
        try {
            return repositories.getPointRepository()
                    .selectRawMetricPointsByIdBetweenTimestamp(metric.getId(), interval.getFromTime(), interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, String aggregation, Granularity granularity, Interval interval, Tag... tags) {
        try {
            return repositories.getPointRepository()
                    .selectMetricsPointsByNameAndAggregationAndTagsBetweenTimestamp(metricName,
                                                                                    aggregation,
                                                                                    granularity,
                                                                                    Tag.toMap(tags),
                                                                                    interval.getFromTime(),
                                                                                    interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, String aggregation, Granularity granularity, int quantity, Tag... tags) {
        try {
            return repositories.getPointRepository()
                    .selectLastMetricsPointsByNameAndAggregationAndTags(metricName, aggregation, granularity, Tag.toMap(tags), quantity);
        } finally {
            closeConnection();
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Granularity granularity, Interval interval, Tag... tags) {
        try {
            return repositories.getPointRepository()
                    .selectMetricsPointsByNameAndTagsBetweenTimestamp(metricName, granularity, Tag.toMap(tags), interval.getFromTime(), interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Granularity granularity, int quantity, Tag... tags) {
        try {
            return repositories.getPointRepository()
                    .selectLastMetricsPointsByNameAndTags(metricName, granularity, Tag.toMap(tags), quantity);
        } finally {
            closeConnection();
        }
    }

    public <T> List<MetricPoints<T>> selectMetricsPoints(String metricName, Interval interval, Tag... tags) {
        try {
            return repositories.getPointRepository()
                    .selectRawMetricsPointsByNameAndTagsBetweenTimestamp(metricName, Tag.toMap(tags), interval.getFromTime(), interval.getUntilTime());
        } finally {
            closeConnection();
        }
    }

    public <T> void upsertMetricPoint(MetricPoint<T> metricPoint) {
        if (metricPoint == null || metricPoint.getPoint() == null) {
            throw new PugIllegalArgumentException("Cannot upsert a null metric point");
        }

        Metric<T> metric = metricPoint.getMetric();

        if (metric == null) {
            throw new PugIllegalArgumentException("Cannot upsert a null metric");
        }

        MetricRepository metricRepository = repositories.getMetricRepository();
        PointRepository pointRepository = repositories.getPointRepository();

        try {
            if (metricRepository.notExistsMetric(metric)) {
                metricRepository.insertMetric(metric);
            }

            pointRepository.upsertMetricPoint(metricPoint);

            commitConnection();
        } catch (PugException e) {
            rollbackConnection();
            throw e;
        } finally {
            closeConnection();
        }
    }

    public <T> void upsertMetricPoint(Metric<T> metric, Point<T> point) {
        upsertMetricPoint(MetricPoint.of(metric, point));
    }

    public void registerRollUps(String metricName, Aggregation<?> aggregation, Retention retention, Granularity... granularities) {
        rollUpScheduler.registerRollUps(metricName, aggregation, retention, granularities);
    }

    public void addRollUpListener(String metricName, String aggregationName, Granularity granularity, RollUpListener listener) {
        rollUpScheduler.addRollUpListener(metricName, aggregationName, granularity, listener);
    }

    public RollUpListener removeRollUpListener(String metricName, String aggregationName, Granularity granularity) {
        return rollUpScheduler.removeRollUpListener(metricName, aggregationName, granularity);
    }

    protected abstract void closeConnection();

    protected abstract void commitConnection();

    protected abstract void rollbackConnection();

}
