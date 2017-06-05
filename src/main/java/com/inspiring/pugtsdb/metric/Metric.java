package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.exception.PugNotImplementedException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import static com.inspiring.pugtsdb.util.MurmurHash3.murmurhash3_x86_32;
import static com.inspiring.pugtsdb.util.Strings.isBlank;
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
        Type genericType = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        String example = "class StringMetric extends Metric<String> {\n"
                + "\n"
                + "    public StringMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {\n"
                + "        this(name, tags, timestamp, new String(value));\n"
                + "    }\n"
                + "\n"
                + "    public StringMetric(String name, Map<String, String> tags, Long timestamp, String value) {\n"
                + "        super(name, tags, timestamp, value);\n"
                + "    }\n"
                + "\n"
                + "    @Override\n"
                + "    public byte[] getValueAsBytes() {\n"
                + "        return value.getBytes();\n"
                + "    }\n"
                + "}";
        String constructorArgs = "String name, Map<String, String> tags, Long timestamp, byte[] value";
        String message = MessageFormat.format(
                "Constructor {0}({1}) must be implemented with proper value conversion from byte[] to {2}.\nExample:\n{3}",
                getClass().getTypeName(),
                constructorArgs,
                genericType.getTypeName(),
                example);
        throw new PugNotImplementedException(message);
    }

    public Metric(String name, Map<String, String> tags, Long timestamp, T value) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Metric name cannot be blank");
        }

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

    @Override
    public String toString() {
        return "Metric{" +
                "id=" + getId() +
                ", type='" + getClass().getTypeName() + '\'' +
                ", name='" + name + '\'' +
                ", tags=" + tags +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
