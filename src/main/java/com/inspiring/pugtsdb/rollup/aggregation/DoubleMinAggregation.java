package com.inspiring.pugtsdb.rollup.aggregation;

import static java.lang.Math.min;

public class DoubleMinAggregation extends Aggregation<Double> {

    public DoubleMinAggregation() {
        super("min");
    }

    @Override
    public Double aggregate(Double value1, Double value2) {
        return computeIfNonNullValues(value1, value2, (aDouble1, aDouble2) -> min(aDouble1, aDouble2));
    }
}
