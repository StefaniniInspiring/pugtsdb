package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.purge.RawPointPurger;
import com.inspiring.pugtsdb.time.Granularity;
import com.inspiring.pugtsdb.time.Retention;
import com.inspiring.pugtsdb.util.GlobPattern;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
    private final Map<Pattern, List<RollUpBuilder<?>>> rollUpBuildersByGlob = new HashMap<>();
    private final Map<String, List<ScheduledFuture<?>>> scheduledRollUps = new HashMap<>();
    private final Repositories repositories;

    public RollUpScheduler(Repositories repositories) {
        this.repositories = repositories;
        scheduleRawPurger();
    }

    public void registerRollUp(String metricName, Aggregation<Object> aggregation, Retention retention) {
        List<RollUpBuilder<?>> rollUpBuilders = prepareRollUps(metricName, aggregation, retention);

        if (isGlob(metricName)) {
            scheduledRollUps.values()
                    .stream()
                    .flatMap(List::stream)
                    .forEach(scheduledRollUp -> scheduledRollUp.cancel(false));
            scheduledRollUps.clear();

            for (String name : repositories.getMetricRepository().selectMetricNames()) {
                List<ScheduledFuture<?>> schedules = rollUpBuildersByGlob.entrySet()
                        .stream()
                        .filter(entry -> patternMatches(name, entry.getKey()))
                        .reduce(this::mostSpecificPattern)
                        .get()
                        .getValue()
                        .stream()
                        .map(rollUpBuilder -> rollUpBuilder.build(name))
                        .map(rollUp -> scheduleRollUp(rollUp))
                        .collect(toList());
                scheduledRollUps.put(metricName, schedules);
            }
        } else {
            scheduledRollUps.compute(metricName,
                                     (key, oldSchedules) -> {
                                         if (oldSchedules != null) {
                                             oldSchedules.forEach(scheduledRollUp -> scheduledRollUp.cancel(true));
                                         }

                                         return rollUpBuilders.stream()
                                                 .map(rollUpBuilder -> rollUpBuilder.build(metricName))
                                                 .map(rollUp -> scheduleRollUp(rollUp))
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

    private List<RollUpBuilder<?>> prepareRollUps(String metricName, Aggregation<Object> aggregation, Retention retention) {
        final AtomicReference<Granularity> sourceGranularity = new AtomicReference<>(null);

        List<RollUpBuilder<?>> curBuilders = rollUpBuildersByGlob.computeIfAbsent(GlobPattern.compile(metricName), pattern -> new ArrayList<>());
        List<RollUpBuilder<?>> newBuilders = Stream.of(Granularity.values())
                .map(targetGranularity -> new RollUpBuilder<>(aggregation,
                                                              sourceGranularity.getAndSet(targetGranularity),
                                                              targetGranularity,
                                                              retention))
                .collect(toList());
        curBuilders.removeIf(builder -> builder.aggregation.getName().equals(aggregation.getName()));
        curBuilders.addAll(newBuilders);

        return curBuilders;
    }

    private boolean patternMatches(String name, Pattern pattern) {
        return pattern.matcher(name).matches();
    }

    private <T> Entry<Pattern, T> mostSpecificPattern(Entry<Pattern, T> entry1, Entry<Pattern, T> entry2) {
        return entry1 != null && entry1.getKey().pattern().length() > entry2.getKey().pattern().length() ? entry1 : entry2;
    }

    private ScheduledFuture<?> scheduleRollUp(RollUp<?> rollUp) {
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

    private class RollUpBuilder<T> {

        final Aggregation<T> aggregation;
        final Granularity sourceGranularity;
        final Granularity targetGranularity;
        final Retention retention;

        RollUpBuilder(Aggregation<T> aggregation, Granularity sourceGranularity, Granularity targetGranularity, Retention retention) {
            this.aggregation = aggregation;
            this.sourceGranularity = sourceGranularity;
            this.targetGranularity = targetGranularity;
            this.retention = retention;
        }

        RollUp<T> build(String metricName) {
            return new RollUp<>(metricName, aggregation, sourceGranularity, targetGranularity, retention, repositories);
        }
    }
}
