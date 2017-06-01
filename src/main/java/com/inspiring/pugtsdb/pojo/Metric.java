package com.inspiring.pugtsdb.pojo;

import java.util.HashMap;
import java.util.Map;

import static com.inspiring.pugtsdb.util.MurmurHash3.murmurhash3_x86_32;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public abstract class Metric<T> {

    protected final Integer id;
    protected final String name;
    protected final Map<String, String> tags;
    protected final Long timestamp;
    protected final T value;

    public Metric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        this.value = getValueFromBytes(value);
        this.timestamp = timestamp != null ? timestamp : currentTimeMillis();
        this.tags = tags != null ? unmodifiableMap(new HashMap<>(tags)) : emptyMap();
        this.name = name;
        String hashString = name + ":" + this.tags;
        this.id = murmurhash3_x86_32(hashString, 0, hashString.length(), hashString.length());
    }

    public Metric(String name, Map<String, String> tags, Long timestamp, T value) {
        this.value = value;
        this.timestamp = timestamp != null ? timestamp : currentTimeMillis();
        this.tags = tags != null ? unmodifiableMap(new HashMap<>(tags)) : emptyMap();
        this.name = name;
        String hashString = name + ":" + this.tags;
        this.id = murmurhash3_x86_32(hashString, 0, hashString.length(), hashString.length());
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

    public T getValue() {
        return value;
    }

    public abstract byte[] getValueAsBytes();

    public abstract T getValueFromBytes(byte[] bytes);

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
