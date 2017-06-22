package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

public class LongMinAggregation extends Aggregation<Long> {

    public LongMinAggregation() {
        super("min");
    }

    @Override
    public Long aggregate(List<Long> values) {
        return values.stream()
                .min(nullsLast(naturalOrder()))
                .orElse(0L);
    }
}
