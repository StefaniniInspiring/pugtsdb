package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Retention;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawPointPurger extends PointPurger {

    private static final Logger log = LoggerFactory.getLogger(RawPointPurger.class);

    public RawPointPurger(PointRepository pointRepository) {
       super(pointRepository, Retention.of(5, ChronoUnit.SECONDS));
    }

    @Override
    public void run() {
        try {
            pointRepository.deleteRawPointsBeforeTime(lastValidTime());
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
