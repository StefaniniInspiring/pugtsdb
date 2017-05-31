package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class StringMetric extends Metric<String> {

    public StringMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromUtf8String(value);
    }

    @Override
    public void setValueFromBytes(byte[] bytes) {
        this.value = Bytes.toUtf8String(bytes);
    }
}
