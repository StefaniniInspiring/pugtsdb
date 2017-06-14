package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.time.Retention;
import java.time.ZonedDateTime;

public abstract class DataPurger implements Runnable {

    protected final DataRepository dataRepository;
    protected final Retention retention;

    public DataPurger(DataRepository dataRepository, Retention retention) {
        this.dataRepository = dataRepository;
        this.retention = retention;
    }


    protected long lastValidTime() {
        ZonedDateTime now = ZonedDateTime.now().truncatedTo(retention.getUnit());
        ZonedDateTime lastValidDate = now.minus(retention);

        return lastValidDate.toInstant().toEpochMilli();
    }
}
