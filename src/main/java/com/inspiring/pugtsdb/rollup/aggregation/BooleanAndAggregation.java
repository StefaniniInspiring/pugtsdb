package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class BooleanAndAggregation extends Aggregation<Boolean> {

    public BooleanAndAggregation() {
        super("and");
    }

    @Override
    public Boolean aggregate(List<Boolean> values) {
        return values.stream()
                .reduce((aBoolean, aBoolean2) -> aBoolean && aBoolean2)
                .orElse(false);
    }
}
