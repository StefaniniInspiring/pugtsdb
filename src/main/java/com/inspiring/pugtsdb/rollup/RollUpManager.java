package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.RawDataPurger;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class RollUpManager {

    private final ScheduledExecutorService rollupThreadPool = newScheduledThreadPool(max(4, Runtime.getRuntime().availableProcessors() * 2));

    public RollUpManager(Repositories repositories) {
        rollupThreadPool.scheduleAtFixedRate(new RawDataPurger(repositories.getDataRepository()), 30, 5, TimeUnit.SECONDS);
    }

    public void registerRollUp(String metricName, Aggregation<?> aggregation, Retention retention) {

    }

    public void stop() {
        rollupThreadPool.shutdown();

        try {
            if (rollupThreadPool.awaitTermination(10, TimeUnit.SECONDS) == false) {
                rollupThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            rollupThreadPool.shutdownNow();
        }
    }
}
