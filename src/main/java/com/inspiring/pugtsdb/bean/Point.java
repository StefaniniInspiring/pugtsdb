package com.inspiring.pugtsdb.bean;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;

public class Point<T> {

    private final long timestamp;
    private final T value;

    public Point(long timestamp, T value) {
        if (value == null) {
            throw new PugIllegalArgumentException("Point value cannot be null");
        }

        this.timestamp = timestamp;
        this.value = value;
    }

    public static <V> Point<V> of(long timestamp, V value) {
        return new Point<>(timestamp, value);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Point{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
