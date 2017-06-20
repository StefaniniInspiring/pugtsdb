package com.inspiring.pugtsdb.rollup.aggregation;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import java.util.function.BiFunction;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public abstract class Aggregation<T> {

    private final String name;

    public Aggregation(String name) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Aggregation name cannot be blank");
        }

        this.name = name;
    }

    public final String getName() {
        return name;
    }

    protected T computeIfNonNullValues(T value1, T value2, BiFunction<T, T, T> aggregationFunction) {
        if (value1 == null) {
            return value2;
        }

        if (value2 == null) {
            return value1;
        }

        return aggregationFunction.apply(value1, value2);
    }

    public abstract T aggregate(T value1, T value2);
}
