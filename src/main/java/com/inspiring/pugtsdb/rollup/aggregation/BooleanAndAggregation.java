package com.inspiring.pugtsdb.rollup.aggregation;

public class BooleanAndAggregation extends Aggregation<Boolean> {

    public BooleanAndAggregation() {
        super("and");
    }

    @Override
    public Boolean aggregate(Boolean value1, Boolean value2) {
        return computeIfNonNullValues(value1, value2, (aBoolean, aBoolean2) -> aBoolean && aBoolean2);
    }
}
