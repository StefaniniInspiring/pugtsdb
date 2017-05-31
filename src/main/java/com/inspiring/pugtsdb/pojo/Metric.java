package com.inspiring.pugtsdb.pojo;

import java.util.HashMap;
import java.util.Map;

import static com.inspiring.pugtsdb.util.MurmurHash3.murmurhash3_x86_32;
import static java.util.Collections.unmodifiableMap;

public abstract class Metric<T> {

    final Integer id;
    final String name;
    final Map<String, String> tags;
    Long timestamp;
    T value;

    public Metric(String name, Map<String, String> tags) {
        String hashString = name + ":" + tags;
        this.id = murmurhash3_x86_32(hashString, 0, hashString.length(), hashString.length());
        this.name = name;
        this.tags = unmodifiableMap(new HashMap<>(tags));
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public abstract byte[] getValueAsBytes();

    public abstract void setValueFromBytes(byte[] bytes);

    @Override
    public String toString() {
        return "Metric{" +
                "id=" + getId() +
                ", type='" + getClass().getName() + '\'' +
                ", name='" + name + '\'' +
                ", tags=" + tags +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
