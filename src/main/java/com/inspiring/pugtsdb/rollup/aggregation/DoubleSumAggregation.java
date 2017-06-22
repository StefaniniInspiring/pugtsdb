package com.inspiring.pugtsdb.rollup.aggregation;

import java.util.List;

public class DoubleSumAggregation extends Aggregation<Double> {

    public DoubleSumAggregation() {
        super("sum");
    }

    @Override
    public Double aggregate(List<Double> values) {
        return values.stream()
                .mapToDouble(Double::doubleValue)
                .sum();
    }
}
