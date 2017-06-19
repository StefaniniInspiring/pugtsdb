package com.inspiring.pugtsdb.rollup.aggregation;

import static java.lang.Math.max;

public class DoubleMaxAggregation extends Aggregation<Double> {

    public DoubleMaxAggregation() {
        super("max");
    }

    @Override
    public Double aggregate(Double value1, Double value2) {
        return computeIfNonNullValues(value1, value2, (aDouble1, aDouble2) -> max(aDouble1, aDouble2));
    }
}
