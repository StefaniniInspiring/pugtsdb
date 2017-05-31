package com.inspiring.pugtsdb.pojo;

import com.inspiring.pugtsdb.util.Bytes;
import java.util.Map;

public class LongMetric extends Metric<Long> {

    public LongMetric(String name, Map<String, String> tags) {
        super(name, tags);
    }

    @Override
    public byte[] getValueAsBytes() {
        return Bytes.fromLong(value);
    }

    @Override
    public void setValueFromBytes(byte[] bytes) {
        this.value = Bytes.toLong(bytes);
    }

}
