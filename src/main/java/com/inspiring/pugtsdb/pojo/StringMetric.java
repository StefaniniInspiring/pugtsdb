package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class StringMetric extends Metric<String> {

    public StringMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, value);
    }

    public StringMetric(String name, Map<String, String> tags, Long timestamp, String value) {
        super(name, tags, timestamp, value);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromUtf8String(value);
    }

    @Override
    public String getValueFromBytes(byte[] bytes) {
        return Bytes.toUtf8String(bytes);
    }
}
