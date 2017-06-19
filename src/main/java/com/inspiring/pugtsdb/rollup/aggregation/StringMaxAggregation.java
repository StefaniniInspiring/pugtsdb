package com.inspiring.pugtsdb.rollup.aggregation;

import static java.util.Comparator.naturalOrder;
import static java.util.Objects.compare;

public class StringMaxAggregation extends Aggregation<String> {

    public StringMaxAggregation() {
        super("max");
    }

    @Override
    public String aggregate(String value1, String value2) {
        return computeIfNonNullValues(value1, value2, (s, s2) -> compare(s, s2, naturalOrder()) < 0 ? s2 : s);
    }
}
