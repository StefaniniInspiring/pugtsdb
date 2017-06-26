package com.inspiring.pugtsdb.rollup.listen;

import com.inspiring.pugtsdb.time.Granularity;

public class RollUpEvent {

    private final String metricName;
    private final String aggregationName;
    private final Granularity sourceGranularity;
    private final Granularity targetGranularity;

    public RollUpEvent(String metricName, String aggregationName, Granularity sourceGranularity, Granularity targetGranularity) {
        this.metricName = metricName;
        this.aggregationName = aggregationName;
        this.sourceGranularity = sourceGranularity;
        this.targetGranularity = targetGranularity;
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

    @Override
    public String toString() {
        return "RollUpEvent{" +
                "metricName='" + metricName + '\'' +
                ", aggregationName='" + aggregationName + '\'' +
                ", sourceGranularity=" + sourceGranularity +
                ", targetGranularity=" + targetGranularity +
                '}';
    }
}
