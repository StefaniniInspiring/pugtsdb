package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class BooleanMetric extends Metric<Boolean> {

    public BooleanMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] valueToBytes(Boolean value) {
        return Bytes.fromBoolean(value);
    }

    @Override
    public Boolean valueFromBytes(byte[] bytes) {
        return Bytes.toBoolean(bytes);
    }
}
