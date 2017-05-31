package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class DoubleMetric extends Metric<Double> {

    public DoubleMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromDouble(value);
    }

    @Override
    public void setValueFromBytes(byte[] bytes) {
        this.value = Bytes.toDouble(bytes);
    }
}
