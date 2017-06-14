package com.inspiring.pugtsdb.time;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public enum Granularity {

    ONE_SECOND("1s", ChronoUnit.SECONDS),
    ONE_MINUTE("1m", ChronoUnit.MINUTES),
    ONE_HOUR("1h", ChronoUnit.HOURS),
    ONE_DAY("1d", ChronoUnit.DAYS),
    ONE_MONTH("1mo", ChronoUnit.MONTHS),
    ONE_YEAR("1y", ChronoUnit.YEARS);

    private final String string;
    private final TemporalUnit unit;

    Granularity(String string, TemporalUnit unit) {
        this.string = string;
        this.unit = unit;
    }

    @Override
    public String toString() {
        return string;
    }

    public TemporalUnit getUnit() {
        return unit;
    }
}
