package com.inspiring.pugtsdb.rollup;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Retention implements TemporalAmount {

    private static final EnumSet<ChronoUnit> units = EnumSet.of(ChronoUnit.SECONDS,
                                                                ChronoUnit.MINUTES,
                                                                ChronoUnit.HOURS,
                                                                ChronoUnit.DAYS,
                                                                ChronoUnit.WEEKS,
                                                                ChronoUnit.MONTHS,
                                                                ChronoUnit.YEARS);

    private final long value;
    private final ChronoUnit unit;
    private final Period period;
    private final Duration duration;

    public Retention(long value, ChronoUnit unit) {
        this.value = value;
        this.unit = unit;
        this.period = toPeriod();
        this.duration = toDuration();
    }

    public static Retention of(long value, ChronoUnit unit) {
        return new Retention(value, unit);
    }

    private Duration toDuration() {
        switch (unit) {
            case SECONDS:
                return Duration.ofSeconds(value);
            case MINUTES:
                return Duration.ofMinutes(value);
            case HOURS:
                return Duration.ofHours(value);
            default:
                return Duration.ZERO;
        }
    }

    private Period toPeriod() {
        switch (unit) {
            case DAYS:
                return Period.ofDays((int) value);
            case WEEKS:
                return Period.ofWeeks((int) value);
            case MONTHS:
                return Period.ofMonths((int) value);
            case YEARS:
                return Period.ofYears((int) value);
            default:
                return Period.ZERO;
        }
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
        return duration.addTo(period.addTo(temporal));
    }

    @Override
    public Temporal subtractFrom(Temporal temporal) {
        return duration.subtractFrom(period.subtractFrom(temporal));
    }
}
