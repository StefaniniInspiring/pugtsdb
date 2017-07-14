package com.inspiring.pugtsdb.time;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.lang.System.currentTimeMillis;

public class Interval {

    private final long fromTime;
    private final long untilTime;

    public Interval(long fromInclusiveTime, long untilExclusiveTime) {
        this.fromTime = fromInclusiveTime;
        this.untilTime = untilExclusiveTime;
    }

    public long getFromTime() {
        return fromTime;
    }

    public long getUntilTime() {
        return untilTime;
    }

    @Override
    public String toString() {
        return "Interval{" +
                "fromTime=" + fromTime +
                ", untilTime=" + untilTime +
                '}';
    }

    public static Builder until(long time) {
        return new Builder(time, false);
    }

    public static Builder ofLast(long amount, ChronoUnit unit) {
        return ofLastSeconds(toSeconds(amount, unit));
    }

    public static Builder ofLast(Granularity granularity) {
        return ofLastSeconds(toSeconds(granularity.getValue(), granularity.getUnit()));
    }

    public static Builder ofLastYears(long amount) {
        return ofLastSeconds(toSeconds(amount, ChronoUnit.YEARS));
    }

    public static Builder ofLastMonths(long amount) {
        return ofLastSeconds(toSeconds(amount, ChronoUnit.MONTHS));
    }

    public static Builder ofLastWeeks(long amount) {
        return ofLastSeconds(toSeconds(amount, ChronoUnit.WEEKS));
    }

    public static Builder ofLastDays(long amount) {
        return ofLastSeconds(toSeconds(amount, ChronoUnit.DAYS));
    }

    public static Builder ofLastHours(long amount) {
        return ofLastSeconds(toSeconds(amount, ChronoUnit.HOURS));
    }

    public static Builder ofLastSeconds(long amount) {
        return new Builder(amount, true);
    }

    private static long toSeconds(long amount, ChronoUnit unit) {
        return unit.getDuration().multipliedBy(amount).getSeconds();
    }

    public static class Builder {

        final long endTimeOrDuration;
        final boolean byDuration;

        Builder(long endTimeOrDuration, boolean byDuration) {
            this.endTimeOrDuration = endTimeOrDuration;
            this.byDuration = byDuration;
        }

        public Interval fromNow() {
            return from(currentTimeMillis());
        }

        public Interval fromNowTruncatedTo(ChronoUnit unit) {
            return from(truncate(currentTimeMillis(), unit));
        }

        public Interval fromYearsAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.YEARS));
        }

        public Interval fromMonthsAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.MONTHS));
        }

        public Interval fromWeeksAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.WEEKS));
        }

        public Interval fromDaysAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.DAYS));
        }

        public Interval fromHoursAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.HOURS));
        }

        public Interval fromMinutesAgo(long amount) {
            return fromSecondsAgo(toSeconds(amount, ChronoUnit.MINUTES));
        }

        public Interval fromSecondsAgo(long amount) {
            return from(Instant.now().minusSeconds(amount).toEpochMilli());
        }

        public Interval from(long time) {
            if (byDuration) {
                long start = Instant.ofEpochMilli(time).minusSeconds(endTimeOrDuration).toEpochMilli();
                long end = time;

                return new Interval(start, end);
            }

            return new Interval(time, endTimeOrDuration);
        }
    }
}
