package com.inspiring.pugtsdb.time;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Retention implements TemporalAmount {

    private static final EnumSet<? extends TemporalUnit> units = EnumSet.of(ChronoUnit.SECONDS,
                                                                            ChronoUnit.MINUTES,
                                                                            ChronoUnit.HOURS,
                                                                            ChronoUnit.DAYS,
                                                                            ChronoUnit.WEEKS,
                                                                            ChronoUnit.MONTHS,
                                                                            ChronoUnit.YEARS);

    private final long value;
    private final ChronoUnit unit;

    public Retention(long value, ChronoUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public static Retention of(long value, ChronoUnit unit) {
        return new Retention(value, unit);
    }

    public ChronoUnit getUnit() {
        return unit;
    }

    @Override
    public long get(TemporalUnit unit) {
        if (this.unit.equals(unit)) {
            return value;
        }

        if (units.contains(unit)) {
            return 0;
        }

        throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
    }

    @Override
    public List<TemporalUnit> getUnits() {
        return new ArrayList<>(units);
    }

    @Override
    public Temporal addTo(Temporal temporal) {
        return temporal.plus(value, unit);
    }

    @Override
    public Temporal subtractFrom(Temporal temporal) {
        return temporal.minus(value, unit);
    }
}
