package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Retention;
import java.time.ZonedDateTime;

import static com.inspiring.pugtsdb.util.Temporals.truncate;

public abstract class PointPurger implements Runnable {

    protected final PointRepository pointRepository;
    protected final Retention retention;

    public PointPurger(PointRepository pointRepository, Retention retention) {
        this.pointRepository = pointRepository;
        this.retention = retention;
    }

    public Retention getRetention() {
        return retention;
    }

    protected long lastValidTime() {
        ZonedDateTime now = truncate(ZonedDateTime.now(), retention.getUnit());
        ZonedDateTime lastValidDate = now.minus(retention);

        return lastValidDate.toInstant().toEpochMilli();
    }
}
