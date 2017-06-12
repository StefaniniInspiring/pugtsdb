package com.inspiring.pugtsdb.rollup.purge;

import com.inspiring.pugtsdb.repository.DataRepository;
import com.inspiring.pugtsdb.rollup.Retention;
import java.time.temporal.ChronoUnit;

public class RawDataPurger extends DataPurger {

    public RawDataPurger(DataRepository dataRepository) {
       super(dataRepository, Retention.of(5, ChronoUnit.SECONDS));
    }

    @Override
    public void run() {
        dataRepository.deleteRawDataBeforeTime(lastValidTime());
    }
}
