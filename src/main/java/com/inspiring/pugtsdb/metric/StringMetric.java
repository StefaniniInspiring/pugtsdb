package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class StringMetric extends Metric<String> {

    public StringMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] toBytes(String value) {
        return Bytes.fromUtf8String(value);
    }

    @Override
    public String fromBytes(byte[] bytes) {
        return Bytes.toUtf8String(bytes);
    }
}
