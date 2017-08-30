package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static com.inspiring.pugtsdb.util.MurmurHash3.murmurhash3_x86_32;
import static com.inspiring.pugtsdb.util.Strings.isBlank;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;

public abstract class Metric<T> {

    public static final int ID_LENGTH = 13;

    protected final String id;
    protected final String name;
    protected final Map<String, String> tags;

    public Metric(String name, Map<String, String> tags) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Metric name cannot be blank");
        }

        this.tags = tags != null ? unmodifiableMap(new TreeMap<>(tags)) : emptyMap();
        this.name = name;
        this.id = newId(name, this.tags);
    }

    public Metric(String id, String name, Map<String, String> tags) {
        this.id = id;
        this.name = name;
        this.tags = tags != null ? unmodifiableMap(new TreeMap<>(tags)) : emptyMap();
    }

    public String getId() {
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

    private static String newId(String name, Map<String, String> tags) {
        String rawId = name.concat(tags.toString());
        String hashId = String.valueOf(murmurhash3_x86_32(rawId, 0, rawId.length(), rawId.length()));
        String rightPad = IntStream.range(0, ID_LENGTH - hashId.length()).mapToObj(value -> "x").collect(joining());

        return hashId.concat(rightPad);
    }
}
