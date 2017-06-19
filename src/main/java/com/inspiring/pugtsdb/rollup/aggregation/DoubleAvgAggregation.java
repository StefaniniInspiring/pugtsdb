package com.inspiring.pugtsdb.rollup.aggregation;

public class DoubleAvgAggregation extends Aggregation<Double> {

    public DoubleAvgAggregation() {
        super("avg");
    }

    @Override
    public Double aggregate(Double value1, Double value2) {
        return computeIfNonNullValues(value1, value2, (aDouble1, aDouble2) -> (aDouble1 + aDouble2) / 2);
    }
}
