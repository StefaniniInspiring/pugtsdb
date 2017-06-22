package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

public class DoubleMinAggregation extends Aggregation<Double> {

    public DoubleMinAggregation() {
        super("min");
    }

    @Override
    public Double aggregate(List<Double> values) {
        return values.stream()
                .min(nullsLast(naturalOrder()))
                .orElse(0D);
    }
}
