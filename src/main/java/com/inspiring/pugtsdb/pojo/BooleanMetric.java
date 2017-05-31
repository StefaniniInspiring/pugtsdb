package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class BooleanMetric extends Metric<Boolean> {

    public BooleanMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromBoolean(value);
    }

    @Override
    public void setValueFromBytes(byte[] bytes) {
        this.value = Bytes.toBoolean(bytes);
    }
}