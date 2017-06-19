package com.inspiring.pugtsdb.rollup.aggregation;

public class DoubleSumAggregation extends Aggregation<Double> {

    public DoubleSumAggregation() {
        super("sum");
    }

    @Override
    public Double aggregate(Double value1, Double value2) {
        return computeIfNonNullValues(value1, value2, (aDouble1, aDouble2) -> aDouble1 + aDouble2);
    }
}
