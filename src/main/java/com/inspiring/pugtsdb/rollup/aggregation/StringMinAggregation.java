package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

public class StringMinAggregation extends Aggregation<String> {

    public StringMinAggregation() {
        super("min");
    }

    @Override
    public String aggregate(List<String> values) {
        return values.stream()
                .min(nullsLast(naturalOrder()))
                .orElse("");
    }
}
