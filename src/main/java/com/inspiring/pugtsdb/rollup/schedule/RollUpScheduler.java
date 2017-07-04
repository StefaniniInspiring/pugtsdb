package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.repository.Repositories;
import com.inspiring.pugtsdb.rollup.RollUp;
import com.inspiring.pugtsdb.rollup.aggregation.Aggregation;
import com.inspiring.pugtsdb.rollup.listen.RollUpListener;
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
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.inspiring.pugtsdb.util.GlobPattern.isGlob;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class RollUpScheduler {

    private static final int INITIAL_DELAY = 5;

    private final ScheduledThreadPool scheduledThreadPool = new ScheduledThreadPool();
    private final Map<Pattern, List<RollUpBuilder<?>>> rollUpBuildersByGlob = new HashMap<>();
    private final Map<String, List<ScheduledRollUp<?>>> scheduledRollUps = new HashMap<>();
    private final Repositories repositories;

    public RollUpScheduler(Repositories repositories) {
        this.repositories = repositories;
        scheduleRawPurger();
    }

    public void registerRollUps(String metricName, Aggregation<?> aggregation, Retention retention) {
        List<RollUpBuilder<?>> rollUpBuilders = prepareRollUps(metricName, aggregation, retention);
        Map<RollUp<?>, RollUpListener> rollUpListeners = new TreeMap<>(comparing((RollUp<?> rollUp) -> rollUp.getMetricName())
                                                                               .thenComparing(RollUp::getAggregation, comparing(Aggregation::getName))
                                                                               .thenComparing(RollUp::getSourceGranularity)
                                                                               .thenComparing(RollUp::getTargetGranularity));

        if (isGlob(metricName)) {
            scheduledRollUps.values()
                    .stream()
                    .flatMap(List::stream)
                    .forEach(scheduledRollUp -> {
                        scheduledRollUp.cancel(false);
                        rollUpListeners.computeIfAbsent(scheduledRollUp.getRollUp(), RollUp::getListener);
                    });
            scheduledRollUps.clear();

            for (String name : repositories.getMetricRepository().selectMetricNames()) {
                List<ScheduledRollUp<?>> schedules = rollUpBuildersByGlob.entrySet()
                        .stream()
                        .filter(entry -> patternMatches(name, entry.getKey()))
                        .reduce(this::mostSpecificPattern)
                        .get()
                        .getValue()
                        .stream()
                        .map(rollUpBuilder -> rollUpBuilder.build(name, rollUpListeners))
                        .map(rollUp -> scheduleRollUp(rollUp))
                        .collect(toList());
                scheduledRollUps.put(metricName, schedules);
            }
        } else {
            scheduledRollUps.compute(metricName,
                                     (key, oldSchedules) -> {
                                         if (oldSchedules != null) {
                                             oldSchedules.forEach(scheduledRollUp -> {
                                                 scheduledRollUp.cancel(true);
                                                 rollUpListeners.computeIfAbsent(scheduledRollUp.getRollUp(), RollUp::getListener);
                                             });
                                         }

                                         return rollUpBuilders.stream()
                                                 .map(rollUpBuilder -> rollUpBuilder.build(metricName, rollUpListeners))
                                                 .map(rollUp -> scheduleRollUp(rollUp))
                                                 .collect(toList());
                                     });
        }
    }

    public void addRollUpListener(String metricName, String aggregationName, Granularity granularity, RollUpListener listener) {
        scheduledRollUps.values()
                .stream()
                .flatMap(List::stream)
                .map(ScheduledRollUp::getRollUp)
                .filter(rollUp -> rollUp.getMetricName().equals(metricName))
                .filter(rollUp -> rollUp.getAggregation().getName().equals(aggregationName))
                .filter(rollUp -> rollUp.getTargetGranularity() == granularity)
                .findFirst()
                .ifPresent(rollUp -> rollUp.setListener(listener));
    }

    public RollUpListener removeRollUpListener(String metricName, String aggregationName, Granularity granularity) {
        RollUp<?> listenedRollUp = scheduledRollUps.values()
                .stream()
                .flatMap(List::stream)
                .map(ScheduledRollUp::getRollUp)
                .filter(rollUp -> rollUp.getMetricName().equals(metricName))
                .filter(rollUp -> rollUp.getAggregation().getName().equals(aggregationName))
                .filter(rollUp -> rollUp.getTargetGranularity() == granularity)
                .findFirst()
                .orElse(null);

        if (listenedRollUp == null) {
            return null;
        }

        RollUpListener listener = listenedRollUp.getListener();
        listenedRollUp.setListener(null);

        return listener;
    }

    private void scheduleRawPurger() {
        RawPointPurger rawPurger = new RawPointPurger(repositories.getPointRepository());
        long purgePeriod = rawPurger.getRetention().getValue();
        ChronoUnit purgUnit = rawPurger.getRetention().getUnit();
        scheduledThreadPool.scheduleAtFixedRate(rawPurger, INITIAL_DELAY, SECONDS, purgePeriod, purgUnit);
    }

    private List<RollUpBuilder<?>> prepareRollUps(String metricName, Aggregation<?> aggregation, Retention retention) {
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

    private <T> ScheduledRollUp<T> scheduleRollUp(RollUp<T> rollUp) {
        long period = rollUp.getTargetGranularity().getValue();
        ChronoUnit unit = rollUp.getTargetGranularity().getUnit();
        ScheduledFuture<?> scheduledFuture = scheduledThreadPool.scheduleAtFixedRate(rollUp, INITIAL_DELAY, SECONDS, period, unit);

        return new ScheduledRollUp<>(rollUp, scheduledFuture);
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

        RollUp<T> build(String metricName, Map<RollUp<?>, RollUpListener> rollUpListeners) {
            RollUp<T> rollUp = new RollUp<>(metricName, aggregation, sourceGranularity, targetGranularity, retention, repositories);
            rollUp.setListener(rollUpListeners.get(rollUp));

            return rollUp;
        }
    }
}
