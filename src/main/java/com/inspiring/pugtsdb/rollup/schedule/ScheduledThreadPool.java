package com.inspiring.pugtsdb.rollup.schedule;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ScheduledThreadPool implements ScheduledExecutorService {

    private final ScheduledExecutorService threadPool = newScheduledThreadPool(max(4, getRuntime().availableProcessors() * 2));

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelayValue, TimeUnit initialDelayUnit, long periodValue, ChronoUnit periodUnit) {
        long initialDelayNanos = initialDelayUnit.toNanos(initialDelayValue);

        switch (periodUnit) {
            case NANOS:
                return scheduleAtFixedRate(command, initialDelayNanos, periodValue, NANOSECONDS);
            case MICROS:
                return scheduleAtFixedRate(command, initialDelayNanos, TimeUnit.MICROSECONDS.toNanos(periodValue), NANOSECONDS);
            case MILLIS:
                return scheduleAtFixedRate(command, initialDelayNanos, TimeUnit.MILLISECONDS.toNanos(periodValue), NANOSECONDS);
            case SECONDS:
                return scheduleAtFixedRate(command, initialDelayNanos, TimeUnit.SECONDS.toNanos(periodValue), NANOSECONDS);
            case MINUTES:
                return scheduleAtFixedRate(command, initialDelayNanos, TimeUnit.MINUTES.toNanos(periodValue), NANOSECONDS);
            case DAYS:
                return scheduleAtFixedRate(command, initialDelayNanos, TimeUnit.HOURS.toNanos(periodValue), NANOSECONDS);
            default:
                Trigger trigger = new ChronoTrigger(periodValue, periodUnit);

                Runnable runnable = () -> {
                    if (trigger.runNow()) {
                        command.run();
                    }
                };

                return scheduleAtFixedRate(runnable, initialDelayNanos, TimeUnit.HOURS.toNanos(1), NANOSECONDS);
        }

    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return threadPool.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return threadPool.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return threadPool.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return threadPool.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        threadPool.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return threadPool.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return threadPool.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return threadPool.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return threadPool.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return threadPool.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return threadPool.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return threadPool.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return threadPool.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                         long timeout,
                                         TimeUnit unit) throws InterruptedException {
        return threadPool.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return threadPool.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return threadPool.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        threadPool.execute(command);
    }
}
