package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.repository.rocks.PointRocksRepository;
import com.inspiring.pugtsdb.repository.rocks.RocksRepositories;
import com.inspiring.pugtsdb.time.Retention;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.currentTimeMillis;

public class ScheduledRocksPointPurger extends ScheduledPointPurger {

    private static final Logger log = LoggerFactory.getLogger(ScheduledRocksPointPurger.class);

    public ScheduledRocksPointPurger(RocksRepositories repositories,
                                     Retention rawRetention,
                                     Retention aggregatedRetention) {
        super(repositories, rawRetention, aggregatedRetention);
    }

    @Override
    public void run() {
        try {
            super.run();
        } finally {
            compactDB();
        }
    }

    private void compactDB() {
        long compactionStartTime = currentTimeMillis();
        log.trace("Compacting RocksDB...");

        try {
            ((PointRocksRepository) pointRepository).compactDB();
        } catch (Exception e) {
            log.error("Cannot compact RocksDB", e);
        }

        long compactionTime = currentTimeMillis() - compactionStartTime;
        log.trace("Compacted RocksDB: Took={}ms", compactionTime);
    }
}
