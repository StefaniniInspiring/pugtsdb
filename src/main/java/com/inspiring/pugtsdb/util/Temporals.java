package com.inspiring.pugtsdb.util;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;

import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoUnit.DAYS;

public class Temporals {

    public static long truncate(long time, ChronoUnit unit) {
        return truncate(Instant.ofEpochMilli(time), unit);
    }

    public static long truncate(Instant instant, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
            case MICROS:
            case MILLIS:
            case SECONDS:
            case MINUTES:
            case HOURS:
            case HALF_DAYS:
            case DAYS:
                return instant.truncatedTo(unit).toEpochMilli();
            case MONTHS:
                return instant.atZone(systemDefault()).truncatedTo(DAYS).withDayOfMonth(1).toEpochSecond() * 1000;
            case YEARS:
                return instant.atZone(systemDefault()).truncatedTo(DAYS).withDayOfYear(1).toEpochSecond() * 1000;
            default:
                throw new UnsupportedTemporalTypeException("Invalid unit for truncation: " + unit);
        }
    }

    public static ZonedDateTime truncate(ZonedDateTime time, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
            case MICROS:
            case MILLIS:
            case SECONDS:
            case MINUTES:
            case HOURS:
            case HALF_DAYS:
            case DAYS:
                return time.truncatedTo(unit);
            case MONTHS:
                return time.truncatedTo(DAYS).withDayOfMonth(1);
            case YEARS:
                return time.truncatedTo(DAYS).withDayOfYear(1);
            default:
                throw new UnsupportedTemporalTypeException("Invalid unit for truncation: " + unit);
        }
    }
}
