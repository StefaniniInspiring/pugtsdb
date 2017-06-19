package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Retention;
import java.time.ZonedDateTime;

public abstract class PointPurger implements Runnable {

    protected final PointRepository pointRepository;
    protected final Retention retention;

    public PointPurger(PointRepository pointRepository, Retention retention) {
        this.pointRepository = pointRepository;
        this.retention = retention;
    }


    protected long lastValidTime() {
        ZonedDateTime now = ZonedDateTime.now().truncatedTo(retention.getUnit());
        ZonedDateTime lastValidDate = now.minus(retention);

        return lastValidDate.toInstant().toEpochMilli();
    }
}
