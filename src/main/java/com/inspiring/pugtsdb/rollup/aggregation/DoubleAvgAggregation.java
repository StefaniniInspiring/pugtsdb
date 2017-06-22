package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class DoubleAvgAggregation extends Aggregation<Double> {

    public DoubleAvgAggregation() {
        super("avg");
    }

    @Override
    public Double aggregate(List<Double> values) {
        return values.stream()
                .mapToDouble(Double::doubleValue)
                .sum()
                / values.size();
    }
}
