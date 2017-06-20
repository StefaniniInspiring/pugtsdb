package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;

public class AggregatedPointPurger extends PointPurger {

    private final String metricName;
    private final Granularity granularity;
    private final String aggregation;

    public AggregatedPointPurger(String metricName,
                                 Aggregation<?> aggregation,
                                 Granularity granularity,
                                 Retention retention,
                                 PointRepository pointRepository) {
        super(pointRepository, retention);
        this.metricName = metricName;
        this.granularity = granularity;
        this.aggregation = aggregation.toString();
    }

    @Override
    public void run() {
        try {
            pointRepository.deletePointsByNameAndAggregationBeforeTime(metricName, aggregation, granularity, lastValidTime());
            pointRepository.getConnection().commit();
        } catch (Exception e) {
            pointRepository.getConnection().rollback();
        } finally {
            pointRepository.getConnection().close();
        }
    }
}
