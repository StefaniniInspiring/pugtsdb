package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class BooleanMetric extends Metric<Boolean> {

    public BooleanMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        this(name, tags, timestamp, Bytes.toBoolean(value));
    }

    public BooleanMetric(String name, Map<String, String> tags, Long timestamp, Boolean value) {
        super(name, tags, timestamp, value);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromBoolean(value);
    }
}
