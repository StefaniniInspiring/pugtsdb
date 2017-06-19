package com.inspiring.pugtsdb.rollup.aggregation;

import static java.lang.Math.max;

public class LongMaxAggregation extends Aggregation<Long> {

    public LongMaxAggregation() {
        super("max");
    }

    @Override
    public Long aggregate(Long value1, Long value2) {
        return computeIfNonNullValues(value1, value2, (aLong, aLong2) -> max(aLong, aLong2));
    }
}
