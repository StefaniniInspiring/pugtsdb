package com.inspiring.pugtsdb.repository.rocks.bean;

import com.inspiring.pugtsdb.metric.Metric;
import com.inspiring.pugtsdb.util.Strings;

import static com.inspiring.pugtsdb.util.Serializer.deserialize;
import static com.inspiring.pugtsdb.util.Serializer.serialize;

public class PointId {

    public String metricId;
    public Long timestamp;

    public static PointId of(String metricId, Long timestamp) {
        PointId pointId = new PointId();
        pointId.metricId = metricId;
        pointId.timestamp = timestamp;

        return pointId;
    }

    public static PointId of(String metricId, String timestamp) {
        return of(metricId, Long.valueOf(timestamp));
    }

    public static PointId from(byte[] bytes) {
        return bytes == null ? null : from(deserialize(bytes, String.class));
    }

    public static PointId from(String string) {
        return of(string.substring(0, Metric.ID_LENGTH), string.substring(Metric.ID_LENGTH));
    }

    public byte[] toBytes() {
        return serialize(toString());
    }

    @Override
    public String toString() {
        return metricId.concat(Strings.format(timestamp, Metric.ID_LENGTH));
    }
}
