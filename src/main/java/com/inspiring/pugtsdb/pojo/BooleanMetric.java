package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class BooleanMetric extends Metric<Boolean> {

    public BooleanMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, Bytes.toBoolean(value));
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromBoolean(value);
    }
}
