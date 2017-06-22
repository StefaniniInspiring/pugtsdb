package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class LongSumAggregation extends Aggregation<Long> {

    public LongSumAggregation() {
        super("sum");
    }

    @Override
    public Long aggregate(List<Long> values) {
        return values.stream()
                .mapToLong(Long::longValue)
                .sum();
    }
}
