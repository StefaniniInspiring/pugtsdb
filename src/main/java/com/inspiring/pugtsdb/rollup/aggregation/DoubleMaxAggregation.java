package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

public class DoubleMaxAggregation extends Aggregation<Double> {

    public DoubleMaxAggregation() {
        super("max");
    }

    @Override
    public Double aggregate(List<Double> values) {
        return values.stream()
                .max(nullsFirst(naturalOrder()))
                .orElse(0D);
    }
}
