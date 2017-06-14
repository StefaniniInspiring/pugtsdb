package com.inspiring.pugtsdb.bean;

import java.util.Map;

public class MetricPoints {

    private Integer id;
    private String name;
    private Map<String, String> tags;
    private Map<String, Map<Long, Object>> values;

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

    public Map<String, Map<Long, Object>> getValues() {
        return values;
    }

    public void setValues(Map<String, Map<Long, Object>> values) {
        this.values = values;
    }
}
