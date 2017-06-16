package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class LongMetric extends Metric<Long> {

    public LongMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] toBytes(Long value) {
        return Bytes.fromLong(value);
    }

    @Override
    public Long fromBytes(byte[] bytes) {
        return Bytes.toLong(bytes);
    }
}
