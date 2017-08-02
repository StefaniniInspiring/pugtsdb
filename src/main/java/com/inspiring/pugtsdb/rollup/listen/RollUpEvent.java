package com.inspiring.pugtsdb.rollup.listen;

import com.inspiring.pugtsdb.bean.MetricPoints;
import com.inspiring.pugtsdb.time.Granularity;
import java.util.List;

public class RollUpEvent<T> {

    private final String metricName;
    private final String aggregationName;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;
    private final List<MetricPoints<T>> metricsPoints;

    public RollUpEvent(String metricName,
                       String aggregationName,
                       Granularity sourceGranularity,
                       Granularity targetGranularity,
                       List<MetricPoints<T>> metricsPoints) {
        this.metricName = metricName;
        this.aggregationName = aggregationName;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
        this.metricsPoints = metricsPoints;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public Granularity getSourceGranularity() {
        return sourceGranularity;
    }

    public Granularity getTargetGranularity() {
        return targetGranularity;
    }

    public List<MetricPoints<T>> getMetricsPoints() {
        return metricsPoints;
    }

    @Override
    public String toString() {
        return "RollUpEvent{" +
                "metricName='" + metricName + '\'' +
                ", aggregationName='" + aggregationName + '\'' +
                ", sourceGranularity=" + sourceGranularity +
                ", targetGranularity=" + targetGranularity +
                ", metricsPoints=" + metricsPoints +
                '}';
    }
}
