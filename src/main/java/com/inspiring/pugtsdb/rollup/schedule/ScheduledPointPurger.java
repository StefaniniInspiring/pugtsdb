package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.repository.MetricRepository;
import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.inspiring.pugtsdb.util.Temporals.truncate;
import static java.lang.System.currentTimeMillis;

public class ScheduledPointPurger implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledPointPurger.class);
    private static final int PERIOD = 30;

    protected final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    protected final Retention rawRetention;
    protected final Retention aggregatedRetention;
    protected final PointRepository pointRepository;
    protected final MetricRepository metricRepository;

    public ScheduledPointPurger(Repositories repositories, Retention rawRetention, Retention aggregatedRetention) {
        this.rawRetention = rawRetention;
        this.aggregatedRetention = aggregatedRetention;
        this.pointRepository = repositories.getPointRepository();
        this.metricRepository = repositories.getMetricRepository();
        this.scheduler.scheduleAtFixedRate(this, PERIOD, PERIOD, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        long purgeStartTime = currentTimeMillis();
        log.trace("Deleting expired points...");

        Date lastValidAggregatedDate = new Date(lastValidTime(aggregatedRetention));
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
            log.error("Cannot delete expired points", e);
        }

        long purgeTime = currentTimeMillis() - purgeStartTime;
        log.debug("Deleted expired points: Took={}ms", purgeTime);
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdownNow();
    }

    private void deleteRawPoints(String metricName, Date lastValidDate) {
        long deleteStartTime = currentTimeMillis();
        log.trace("Deleting metric '{}' raw points with timestamp before {}...", metricName, lastValidDate);

        try {
            pointRepository.deleteRawPointsByNameBeforeTime(metricName, lastValidDate.getTime());
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            log.trace("Cannot delete metric '{}' raw points with timestamp before {}...", metricName, lastValidDate, e);
            pointRepository.getConnection().rollback();
        } finally {
            pointRepository.getConnection().close();
        }

        long deleteTime = currentTimeMillis() - deleteStartTime;
        log.trace("Deleted metric '{}' raw points with timestamp before {}: Took={}ms", metricName, lastValidDate, deleteTime);
    }

    private void deleteAggregatedPoints(String metricName, String aggregation, Granularity granularity, Date lastValidDate) {
        long deleteStartTime = currentTimeMillis();
        log.trace("Deleting metric '{}' points aggregated as '{}' by '{}' with timestamp before {}...", metricName, aggregation, granularity, lastValidDate);

        try {
            pointRepository.deletePointsByNameAndAggregationBeforeTime(metricName, aggregation, granularity, lastValidDate.getTime());
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            log.error("Cannot delete metric '{}' points aggregated as '{}' by '{}' with timestamp before {}...", metricName, aggregation, granularity, lastValidDate, e);
            pointRepository.getConnection().rollback();
        } finally {
            pointRepository.getConnection().close();
        }

        long deleteTime = currentTimeMillis() - deleteStartTime;
        log.trace("Deleted metric '{}' points aggregated as '{}' by '{}' with timestamp before {}: Took={}ms",
                  metricName,
                  aggregation,
                  granularity,
                  lastValidDate,
                  deleteTime);
    }

    private long lastValidTime(Retention retention) {
        ZonedDateTime now = truncate(ZonedDateTime.now(), retention.getUnit());
        ZonedDateTime lastValidDate = now.minus(retention);

        return lastValidDate.toInstant().toEpochMilli();
    }
}
