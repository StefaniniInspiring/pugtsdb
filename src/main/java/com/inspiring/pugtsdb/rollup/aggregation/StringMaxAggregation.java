package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

public class StringMaxAggregation extends Aggregation<String> {

    public StringMaxAggregation() {
        super("max");
    }

    @Override
    public String aggregate(List<String> values) {
        return values.stream()
                .max(nullsFirst(naturalOrder()))
                .orElse("");
    }
}
