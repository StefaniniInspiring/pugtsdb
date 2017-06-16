package com.inspiring.pugtsdb.rollup.aggregation;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public abstract class Aggregation<T> {

    private final String name;

    protected Aggregation(String name) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Aggregation name cannot be blank");
        }

        this.name = name;
    }

    public final String getName() {
        return name;
    }

    public abstract T aggregate(T value1, T value2);
}
