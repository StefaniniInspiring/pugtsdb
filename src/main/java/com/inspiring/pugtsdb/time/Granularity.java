package com.inspiring.pugtsdb.time;

import java.time.temporal.ChronoUnit;

public enum Granularity {

    ONE_SECOND("1s", 1, ChronoUnit.SECONDS),
    ONE_MINUTE("1m", 1, ChronoUnit.MINUTES),
    ONE_HOUR("1h", 1, ChronoUnit.HOURS),
    ONE_DAY("1d", 1, ChronoUnit.DAYS),
    ONE_MONTH("1mo", 1, ChronoUnit.MONTHS),
    ONE_YEAR("1y", 1, ChronoUnit.YEARS);

    private final String string;
    private final long value;
    private final ChronoUnit unit;

    Granularity(String string, long value, ChronoUnit unit) {
        this.string = string;
        this.value = value;
        this.unit = unit;
    }

    @Override
    public String toString() {
        return string;
    }

    public long getValue() {
        return value;
    }

    public ChronoUnit getUnit() {
        return unit;
    }
}
