package com.inspiring.pugtsdb.repository.rocks.bean;

import com.inspiring.pugtsdb.metric.Metric;
import java.util.HashMap;
import java.util.Map;

public class MetaMetric {

    private Class<? extends Metric> type;
    private Map<String, Map<String, String>> tagsById;

    public MetaMetric() {
        super();
    }

    public MetaMetric(Class<? extends Metric> type) {
        this.type = type;
        this.tagsById = new HashMap<>();
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<? extends Metric> type) {
        this.type = type;
    }

    public Map<String, Map<String, String>> getTagsById() {
        return tagsById;
    }

    public void setTagsById(Map<String, Map<String, String>> tagsById) {
        this.tagsById = tagsById;
    }

    @Override
    public String toString() {
        return "MetaMetric{" +
                "type=" + type +
                ", tagsById=" + tagsById +
                '}';
    }
}
