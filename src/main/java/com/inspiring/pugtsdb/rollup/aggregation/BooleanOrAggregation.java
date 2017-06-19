package com.inspiring.pugtsdb.rollup.aggregation;

public class BooleanOrAggregation extends Aggregation<Boolean> {

    public BooleanOrAggregation() {
        super("or");
    }

    @Override
    public Boolean aggregate(Boolean value1, Boolean value2) {
        return computeIfNonNullValues(value1, value2, (aBoolean, aBoolean2) -> aBoolean || aBoolean2);
    }
}
