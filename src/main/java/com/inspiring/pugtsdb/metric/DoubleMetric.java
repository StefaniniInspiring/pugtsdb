package com.inspiring.pugtsdb.metric;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class DoubleMetric extends Metric<Double> {

    public DoubleMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] valueToBytes(Double value) {
        return Bytes.fromDouble(value);
    }

    @Override
    public Double valueFromBytes(byte[] bytes) {
        return Bytes.toDouble(bytes);
    }
}
