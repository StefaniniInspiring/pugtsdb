package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class LongAvgAggregation extends Aggregation<Long> {

    public LongAvgAggregation() {
        super("avg");
    }

    @Override
    public Long aggregate(List<Long> values) {
        return values.stream()
                .mapToLong(Long::longValue)
                .sum()
                / values.size();
    }
}
