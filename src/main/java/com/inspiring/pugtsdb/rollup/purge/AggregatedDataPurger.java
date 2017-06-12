package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.rollup.Retention;

public class AggregatedDataPurger extends DataPurger {

    private final int metricId;
    private final String aggregationPeriod;

    public AggregatedDataPurger(DataRepository dataRepository, Retention retention, int metricId, String aggregationPeriod) {
        super(dataRepository, retention);
        this.metricId = metricId;
        this.aggregationPeriod = aggregationPeriod;
    }

    @Override
    public void run() {
        dataRepository.deleteAggregatedDataBeforeTime(aggregationPeriod, metricId, lastValidTime());
    }
}
