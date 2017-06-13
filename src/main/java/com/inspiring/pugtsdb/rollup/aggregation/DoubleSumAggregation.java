package com.inspiring.pugtsdb.rollup.aggregation;

public class DoubleSumAggregation implements Aggregation<Double> {

    @Override
    public String getName() {
        return "sum";
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
