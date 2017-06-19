package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.RawPointPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import com.inspiring.pugtsdb.util.GlobPattern;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.GlobPattern.isGlob;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class RollUpScheduler {

    private static final int INITIAL_DELAY = 5;

    private final ScheduledThreadPool scheduledThreadPool = new ScheduledThreadPool();
    private final Map<Pattern, List<RollUp>> rollUpsByGlob = new HashMap<>();
    private final Map<String, List<ScheduledFuture<?>>> scheduledRollUps = new HashMap<>();
    private final Repositories repositories;

    public RollUpScheduler(Repositories repositories) {
        this.repositories = repositories;
        scheduleRawPurger();
    }

    public void registerRollUp(String metricName, Aggregation<Object> aggregation, Retention retention) {
        List<RollUp> rollUps = createRollUps(metricName, aggregation, retention);

        if (isGlob(metricName)) {
            scheduledRollUps.values()
                    .stream()
                    .flatMap(List::stream)
                    .forEach(scheduledRollUp -> scheduledRollUp.cancel(false));
            scheduledRollUps.clear();

            for (String name : repositories.getMetricRepository().selectMetricNames()) {
                List<ScheduledFuture<?>> schedules = rollUpsByGlob.entrySet()
                        .stream()
                        .filter(entry -> patternMatches(name, entry.getKey()))
                        .reduce(null, this::mostSpecificPattern)
                        .getValue()
                        .stream()
                        .map(this::scheduleRollUp)
                        .collect(toList());
                scheduledRollUps.put(metricName, schedules);
            }
        } else {
            scheduledRollUps.compute(metricName,
                                     (key, oldSchedules) -> {
                                         if (oldSchedules != null) {
                                             oldSchedules.forEach(scheduledRollUp -> scheduledRollUp.cancel(true));
                                         }

                                         return rollUps.stream()
                                                 .map(this::scheduleRollUp)
                                                 .collect(toList());
                                     });
        }
    }

    private void scheduleRawPurger() {
        RawPointPurger rawPurger = new RawPointPurger(repositories.getPointRepository());
        long purgePeriod = rawPurger.getRetention().getValue();
        ChronoUnit purgUnit = rawPurger.getRetention().getUnit();
        scheduledThreadPool.scheduleAtFixedRate(rawPurger, INITIAL_DELAY, SECONDS, purgePeriod, purgUnit);
    }

    private List<RollUp> createRollUps(String metricName, Aggregation<Object> aggregation, Retention retention) {
        final AtomicReference<Granularity> sourceGranularity = new AtomicReference<>(null);

        return rollUpsByGlob.compute(GlobPattern.compile(metricName),
                                     (pattern, rollUps) -> Stream.of(Granularity.values())
                                             .map(targetGranularity -> new RollUp<>(metricName,
                                                                                    aggregation,
                                                                                    sourceGranularity.getAndSet(targetGranularity),
                                                                                    targetGranularity,
                                                                                    retention,
                                                                                    repositories))
                                             .collect(toList()));
    }

    private boolean patternMatches(String name, Pattern pattern) {
        return pattern.matcher(name).matches();
    }

    private Entry<Pattern, List<RollUp>> mostSpecificPattern(Entry<Pattern, List<RollUp>> entry1, Entry<Pattern, List<RollUp>> entry2) {
        return entry1 != null && entry1.getKey().pattern().length() > entry2.getKey().pattern().length() ? entry1 : entry2;
    }

    private ScheduledFuture<?> scheduleRollUp(RollUp rollUp) {
        long period = rollUp.getTargetGranularity().getValue();
        ChronoUnit unit = rollUp.getTargetGranularity().getUnit();

        return scheduledThreadPool.scheduleAtFixedRate(rollUp, INITIAL_DELAY, SECONDS, period, unit);
    }

    public void stop() {
        scheduledThreadPool.shutdown();

        try {
            if (scheduledThreadPool.awaitTermination(10, SECONDS) == false) {
                scheduledThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledThreadPool.shutdownNow();
        }
    }
}
