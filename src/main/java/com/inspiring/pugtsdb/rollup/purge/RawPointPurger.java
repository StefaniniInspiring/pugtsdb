package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.time.Retention;
import java.time.temporal.ChronoUnit;

public class RawPointPurger extends PointPurger {

    public RawPointPurger(PointRepository pointRepository) {
       super(pointRepository, Retention.of(5, ChronoUnit.SECONDS));
    }

    @Override
    public void run() {
        try {
            pointRepository.deleteRawPointsBeforeTime(lastValidTime());
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            pointRepository.getConnection().rollback();
        } finally {
            pointRepository.getConnection().close();
        }
    }
}
