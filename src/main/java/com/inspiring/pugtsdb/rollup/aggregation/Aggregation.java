package com.inspiring.pugtsdb.rollup.aggregation;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;

import static com.inspiring.pugtsdb.util.Strings.isBlank;

public abstract class Aggregation<T> {

    public static final String RAW = "raw";

    private final String name;

    protected Aggregation(String name) {
        if (isBlank(name)) {
            throw new PugIllegalArgumentException("Aggregation name cannot be blank");
        }

        if (name.equals(RAW)) {
            throw new PugIllegalArgumentException("Aggregation name '" + RAW + "' is reserved and cannot be used");
        }

        this.name = name;
    }

    public final String getName() {
        return name;
    }

    public abstract T aggregate(T value1, T value2);
}
