package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;

public class AggregatedDataPurger extends DataPurger {

    private final String metricName;
    private final Granularity granularity;
    private final String aggregation;

    public AggregatedDataPurger(String metricName,
                                Aggregation<Object> aggregation,
                                Granularity granularity,
                                Retention retention,
                                DataRepository dataRepository) {
        super(dataRepository, retention);
        this.metricName = metricName;
        this.granularity = granularity;
        this.aggregation = aggregation.toString();
    }

    @Override
    public void run() {
        dataRepository.deleteAggregatedDataBeforeTime(metricName, aggregation, granularity, lastValidTime());
    }
}
