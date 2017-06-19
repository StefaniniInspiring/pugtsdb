package com.inspiring.pugtsdb.rollup.aggregation;

public class LongSumAggregation extends Aggregation<Long> {

    public LongSumAggregation() {
        super("sum");
    }

    @Override
    public Long aggregate(Long value1, Long value2) {
        return computeIfNonNullValues(value1, value2, (aLong, aLong2) -> aLong + aLong2);
    }
}
