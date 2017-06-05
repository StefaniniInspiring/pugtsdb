package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class LongMetric extends Metric<Long> {

    public LongMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, Bytes.toLong(value));
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromLong(value);
    }
}
