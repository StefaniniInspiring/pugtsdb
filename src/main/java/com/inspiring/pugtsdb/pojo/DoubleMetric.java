package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class DoubleMetric extends Metric<Double> {

    public DoubleMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        super(name, tags, timestamp, Bytes.toDouble(value));
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromDouble(value);
    }
}
