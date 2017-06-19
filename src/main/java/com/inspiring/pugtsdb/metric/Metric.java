package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import java.util.HashMap;
import java.util.Map;

import static com.inspiring.pugtsdb.util.MurmurHash3.murmurhash3_x86_32;
import static com.inspiring.pugtsdb.util.Strings.isBlank;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public abstract class Metric<T> {

    protected final Integer id;
    protected final String name;
    protected final Map<String, String> tags;

    public Metric(String name, Map<String, String> tags) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Metric name cannot be blank");
        }

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

    public abstract byte[] valueToBytes(T value);

    public abstract T valueFromBytes(byte[] bytes);

    @Override
    public String toString() {
        return "Metric{" +
                "id=" + getId() +
                ", type='" + getClass().getTypeName() + '\'' +
                ", name='" + name + '\'' +
                ", tags=" + tags +
                '}';
    }
}
