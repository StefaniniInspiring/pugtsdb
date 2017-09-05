package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.rocks.MetricRocksRepository;
import com.inspiring.pugtsdb.repository.rocks.PointRocksRepository;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.currentTimeMillis;

public class RocksPointPurger extends PointPurger implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RocksPointPurger.class);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Retention rawRetention;

    public RocksPointPurger(PointRocksRepository pointRepository, Retention rawRetention, Retention aggregatedRetention) {
        super(pointRepository, aggregatedRetention);
        this.rawRetention = rawRetention;
        this.scheduler.scheduleAtFixedRate(this, 15, 15, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        long purgeStartTime = currentTimeMillis();
        log.trace("Deleting expired points on RocksDB...");

        MetricRocksRepository metricRepository = ((PointRocksRepository) pointRepository).getMetricRepository();
        Date lastValidAggregatedDate = new Date(lastValidTime());
        Date lastValidRawDate = new Date(lastValidTime(rawRetention));

        try {
            for (String metricName : metricRepository.selectMetricNames()) {
                deleteRawPoints(metricName, lastValidRawDate);

                for (Granularity granularity : Granularity.values()) {
                    for (String aggregation : pointRepository.selectAggregationNames(metricName, granularity)) {
                        deleteAggregatedPoints(metricName, aggregation, granularity, lastValidAggregatedDate);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Cannot delete expired points on RocksDB", e);
        } finally {
            compactDB();
        }

        long purgeTime = currentTimeMillis() - purgeStartTime;
        log.debug("Deleted expired points on RocksDB: Took={}ms", purgeTime);
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdownNow();
    }

    private void deleteRawPoints(String metricName, Date lastValidDate) {
        long deleteStartTime = currentTimeMillis();
        log.trace("Deleting metric '{}' raw points with timestamp before {}...", metricName, lastValidDate);

        pointRepository.deleteRawPointsByNameBeforeTime(metricName, lastValidDate.getTime());

        long deleteTime = currentTimeMillis() - deleteStartTime;
        log.trace("Deleted metric '{}' raw points with timestamp before {}: Took={}ms", metricName, lastValidDate, deleteTime);
    }

    private void deleteAggregatedPoints(String metricName, String aggregation, Granularity granularity, Date lastValidDate) {
        long deleteStartTime = currentTimeMillis();
        log.trace("Deleting metric '{}' points aggregated as '{}' by '{}' with timestamp before {}...", metricName, aggregation, granularity, lastValidDate);

        pointRepository.deletePointsByNameAndAggregationBeforeTime(metricName, aggregation, granularity, lastValidDate.getTime());

        long deleteTime = currentTimeMillis() - deleteStartTime;
        log.trace("Deleted metric '{}' points aggregated as '{}' by '{}' with timestamp before {}: Took={}ms",
                  metricName,
                  aggregation,
                  granularity,
                  lastValidDate,
                  deleteTime);
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
