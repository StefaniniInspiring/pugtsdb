package com.inspiring.pugtsdb.bean;

import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import java.util.Map;

public class MetricData {

    private Integer id;
    private String name;
    private Map<String, String> tags;
    private String aggregation;
    private Long timestamp;
    private Object value;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public MetricData merge(MetricData other, Aggregation<Object> aggregation) {
        MetricData merge = new MetricData();
        merge.setId(id);
        merge.setName(name);
        merge.setTags(tags);
        merge.setAggregation(this.aggregation);
        merge.setTimestamp(timestamp);
        merge.setValue(aggregation.aggregate(this.value, other.value));

        return merge;
    }
}
