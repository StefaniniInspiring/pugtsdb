package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class BooleanOrAggregation extends Aggregation<Boolean> {

    public BooleanOrAggregation() {
        super("or");
    }

    @Override
    public Boolean aggregate(List<Boolean> values) {
        return values.stream()
                .reduce((aBoolean, aBoolean2) -> aBoolean || aBoolean2)
                .orElse(false);
    }
}
