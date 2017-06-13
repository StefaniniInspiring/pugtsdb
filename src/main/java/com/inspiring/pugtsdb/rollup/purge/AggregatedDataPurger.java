package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.rollup.Retention;

public class AggregatedDataPurger extends DataPurger {

    private final String metricName;
    private final String aggregationPeriod;

    public AggregatedDataPurger(DataRepository dataRepository, Retention retention, String metricName, String aggregationPeriod) {
        super(dataRepository, retention);
        this.metricName = metricName;
        this.aggregationPeriod = aggregationPeriod;
    }

    @Override
    public void run() {
        dataRepository.deleteAggregatedDataBeforeTime(aggregationPeriod, metricName, lastValidTime());
    }
}
