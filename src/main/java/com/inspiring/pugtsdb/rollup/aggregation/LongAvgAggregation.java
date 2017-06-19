package com.inspiring.pugtsdb.rollup.aggregation;

public class LongAvgAggregation extends Aggregation<Long> {

    public LongAvgAggregation() {
        super("avg");
    }

    @Override
    public Long aggregate(Long value1, Long value2) {
        return computeIfNonNullValues(value1, value2, (aLong, aLong2) -> (aLong + aLong2) / 2);
    }
}
