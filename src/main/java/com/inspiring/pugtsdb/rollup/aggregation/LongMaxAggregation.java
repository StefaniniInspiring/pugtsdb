package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

public class LongMaxAggregation extends Aggregation<Long> {

    public LongMaxAggregation() {
        super("max");
    }

    @Override
    public Long aggregate(List<Long> values) {
        return values.stream()
                .max(nullsFirst(naturalOrder()))
                .orElse(0L);
    }
}
