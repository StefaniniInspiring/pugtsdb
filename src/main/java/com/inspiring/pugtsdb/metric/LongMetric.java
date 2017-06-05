package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class LongMetric extends Metric<Long> {

    public LongMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        this(name, tags, timestamp, Bytes.toLong(value));
    }

    public LongMetric(String name, Map<String, String> tags, Long timestamp, Long value) {
        super(name, tags, timestamp, value);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromLong(value);
    }
}
