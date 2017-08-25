package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Retention;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawPointPurger extends PointPurger {

    private static final Logger log = LoggerFactory.getLogger(RawPointPurger.class);

    private final String metricName;

    public RawPointPurger(String metricName, PointRepository pointRepository, Retention retention) {
        super(pointRepository, retention);
        this.metricName = metricName;
    }

    @Override
    public void run() {
        try {
            pointRepository.deleteRawPointsByNameBeforeTime(metricName, lastValidTime());
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            log.error("Cannot run {}", this, e);
            pointRepository.getConnection().rollback();
        } finally {
            pointRepository.getConnection().close();
        }
    }

    @Override
    public String toString() {
        return "RawPointPurger{" +
                "retention=" + retention +
                '}';
    }
}
