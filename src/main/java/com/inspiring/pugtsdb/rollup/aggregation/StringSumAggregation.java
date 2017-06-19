package com.inspiring.pugtsdb.rollup.aggregation;

public class StringSumAggregation extends Aggregation<String> {

    public StringSumAggregation() {
        super("sum");
    }

    @Override
    public String aggregate(String value1, String value2) {
        return computeIfNonNullValues(value1, value2, (s, s2) -> s.concat(s2));
    }
}
