package com.inspiring.pugtsdb.rollup.aggregation;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import java.util.List;

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

    public abstract T aggregate(List<T> values);

    @Override
    public String toString() {
        return "Aggregation{" +
                "name='" + name + '\'' +
                '}';
    }
}
