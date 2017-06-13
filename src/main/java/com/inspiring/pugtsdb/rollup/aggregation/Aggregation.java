package com.inspiring.pugtsdb.rollup.aggregation;

public interface Aggregation<T> {

    String getName();

    T aggregate(T value1, T value2);
}
