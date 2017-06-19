package com.inspiring.pugtsdb.rollup.schedule;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.Objects.compare;

public class ChronoTrigger implements Trigger {

    private final long rate;
    private final ChronoUnit unit;
    private ZonedDateTime nextExecutionDate;

    public ChronoTrigger(long rate, ChronoUnit unit) {
        this.rate = rate;
        this.unit = unit;
    }

    @Override
    public boolean runNow() {
        ZonedDateTime now = ZonedDateTime.now();

        try {
            return compare(now, nextExecutionDate, nullsFirst(naturalOrder())) >= 0;
        } finally {
            nextExecutionDate = truncate(now, unit).plus(rate, unit);
        }
    }
}
