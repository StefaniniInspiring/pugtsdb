package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class LongMetric extends Metric<Long> {

    public LongMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, value);
    }

    public LongMetric(String name, Map<String, String> tags, Long timestamp, Long value) {
        super(name, tags, timestamp, value);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromLong(value);
    }

    public Long getValueFromBytes(byte[] bytes) {
        return Bytes.toLong(bytes);
    }
}
