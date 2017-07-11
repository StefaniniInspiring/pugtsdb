package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.PointRepository;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregatedPointPurger extends PointPurger {

    private static final Logger log = LoggerFactory.getLogger(AggregatedPointPurger.class);

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
        this.aggregation = aggregation.getName();
    }

    @Override
    public void run() {
        try {
            pointRepository.deletePointsByNameAndAggregationBeforeTime(metricName, aggregation, granularity, lastValidTime());
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
        return "AggregatedPointPurger{" +
                "metricName='" + metricName + '\'' +
                ", granularity=" + granularity +
                ", aggregation='" + aggregation + '\'' +
                '}';
    }
}
