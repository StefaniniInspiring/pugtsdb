package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class StringMetric extends Metric<String> {

    public StringMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, Bytes.toUtf8String(value));
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromUtf8String(value);
    }
}
