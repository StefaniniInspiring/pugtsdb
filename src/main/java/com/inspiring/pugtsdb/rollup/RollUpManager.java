package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.RawDataPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class RollUpManager {

    private final ScheduledExecutorService rollupThreadPool = newScheduledThreadPool(max(4, Runtime.getRuntime().availableProcessors() * 2));
    private final Map<String, List<RollUp>> rollUpsByGlob = new HashMap<>();
    private final Map<String, List<ScheduledFuture<?>>> scheduledRollUps = new HashMap<>();
    private final Repositories repositories;

    public RollUpManager(Repositories repositories) {
        this.repositories = repositories;
        rollupThreadPool.scheduleAtFixedRate(new RawDataPurger(repositories.getDataRepository()), 30, 5, TimeUnit.SECONDS);
    }

    public void registerRollUp(String metricName, Aggregation<Object> aggregation, Retention retention) {
        for (Granularity granularity : Granularity.values()) {
            new RollUp(metricName, aggregation, granularity, retention, repositories);
        }
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
