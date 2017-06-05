package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class DoubleMetric extends Metric<Double> {

    public DoubleMetric(String name, Map<String, String> tags, Long timestamp, byte[] value) {
        this(name, tags, timestamp, Bytes.toDouble(value));
    }

    public DoubleMetric(String name, Map<String, String> tags, Long timestamp, Double value) {
        super(name, tags, timestamp, value);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromDouble(value);
    }
}
