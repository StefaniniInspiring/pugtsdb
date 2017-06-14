package com.inspiring.pugtsdb.rollup;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.RawDataPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import com.inspiring.pugtsdb.util.GlobPattern;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.GlobPattern.isGlob;
import static java.lang.Math.max;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;

public class RollUpManager {

    private final ScheduledExecutorService rollUpThreadPool = newScheduledThreadPool(max(4, Runtime.getRuntime().availableProcessors() * 2));
    private final Map<Pattern, List<RollUp>> rollUpsByGlob = new HashMap<>();
    private final Map<String, List<ScheduledFuture<?>>> scheduledRollUps = new HashMap<>();
    private final Repositories repositories;

    public RollUpManager(Repositories repositories) {
        this.repositories = repositories;
        rollUpThreadPool.scheduleAtFixedRate(new RawDataPurger(repositories.getDataRepository()), 30, 5, TimeUnit.SECONDS);
    }

    public void registerRollUp(String metricName, Aggregation<Object> aggregation, Retention retention) {
        Pattern glob = GlobPattern.compile(metricName);
        List<RollUp> rollUps = Stream.of(Granularity.values())
                .map(granularity -> new RollUp(metricName, aggregation, granularity, retention, repositories))
                .collect(toList());

        if (isGlob(metricName)) {
            rollUpsByGlob.put(glob, rollUps);
            scheduledRollUps.values().stream().flatMap(List::stream).forEach(scheduled -> scheduled.cancel(false));
            scheduledRollUps.clear();

            for (String name : repositories.getMetricRepository().selectMetricNames()) {
                List<ScheduledFuture<?>> schedules = rollUpsByGlob.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().matcher(name).matches())
                        .reduce((entry1, entry2) -> entry1.getKey().pattern().length() > entry2.getKey().pattern().length() ? entry1 : entry2)
                        .map(entry -> entry.getValue())
                        .orElse(emptyList())
                        .stream()
                        .map(rollUp -> rollUpThreadPool.scheduleAtFixedRate(rollUp, 0, 0, TimeUnit.SECONDS))//TODO set period from rollup.granularity
                        .collect(toList());
                scheduledRollUps.put(metricName, schedules);
            }
        } else {
            scheduledRollUps.getOrDefault(metricName, emptyList()).forEach(scheduled -> scheduled.cancel(true));
            scheduledRollUps.remove(metricName);

            List<ScheduledFuture<?>> schedules = rollUps.stream()
                    .map(rollUp -> rollUpThreadPool.scheduleAtFixedRate(rollUp, 0, 0, TimeUnit.SECONDS))//TODO set period from rollup.granularity
                    .collect(toList());

            scheduledRollUps.put(metricName, schedules);
        }
    }

    public void stop() {
        rollUpThreadPool.shutdown();

        try {
            if (rollUpThreadPool.awaitTermination(10, TimeUnit.SECONDS) == false) {
                rollUpThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            rollUpThreadPool.shutdownNow();
        }
    }
}
