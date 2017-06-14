package com.inspiring.pugtsdb.rollup.aggregation;

public class DoubleSumAggregation extends Aggregation<Double> {

    protected DoubleSumAggregation(String name) {
        super("sum");
    }

    @Override
    public Double aggregate(Double value1, Double value2) {
        if (value1 == null) {
            return value2 == null ? null : value2;
        }

        if (value2 == null) {
            return value1;
        }

        return value1 + value2;
    }
}
