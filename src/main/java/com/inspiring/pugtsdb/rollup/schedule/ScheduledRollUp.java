package com.inspiring.pugtsdb.rollup.schedule;

import com.inspiring.pugtsdb.rollup.RollUp;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ScheduledRollUp<T> implements ScheduledFuture<Object> {

    private final RollUp<T> rollUp;
    private final ScheduledFuture<?> scheduledFuture;

    public ScheduledRollUp(RollUp<T> rollUp, ScheduledFuture<?> scheduledFuture) {
        this.rollUp = rollUp;
        this.scheduledFuture = scheduledFuture;
    }

    public RollUp<T> getRollUp() {
        return rollUp;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return scheduledFuture.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
        return scheduledFuture.compareTo(o);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return scheduledFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return scheduledFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return scheduledFuture.isDone();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        return scheduledFuture.get();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return scheduledFuture.get(timeout, unit);
    }
}
