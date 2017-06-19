package com.inspiring.pugtsdb.rollup.aggregation;

import static java.lang.Math.min;

public class LongMinAggregation extends Aggregation<Long> {

    public LongMinAggregation() {
        super("min");
    }

    @Override
    public Long aggregate(Long value1, Long value2) {
        return computeIfNonNullValues(value1, value2, (aLong, aLong2) -> min(aLong, aLong2));
    }
}
