package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class StringSumAggregation extends Aggregation<String> {

    public StringSumAggregation() {
        super("sum");
    }

    @Override
    public String aggregate(List<String> values) {
        return values.stream()
                .reduce((s, s2) -> s.concat(s2))
                .orElse("");
    }
}
