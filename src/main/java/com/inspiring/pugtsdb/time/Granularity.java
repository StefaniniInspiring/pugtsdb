package com.inspiring.pugtsdb.time;

import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import java.time.temporal.ChronoUnit;

public enum Granularity {

    ONE_SECOND("1s", 1, ChronoUnit.SECONDS),
    HALF_MINUTE("30s", 30, ChronoUnit.SECONDS),
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

    public static Granularity fromString(String string) {
        for (Granularity granularity : values()) {
            if (granularity.string.equals(string)) {
                return granularity;
            }
        }

        return valueOf(string);
    }

    public static Granularity valueOf(long value, ChronoUnit unit) {
        for (Granularity granularity : values()) {
            if (granularity.value == value
                    && granularity.unit == unit) {
                return granularity;
            }
        }

        throw new PugIllegalArgumentException("No granularity found with value " + value + " and unit " + unit);
    }
}
