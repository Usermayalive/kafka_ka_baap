package com.simplekafka.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metric collector for Dead Letter Queue operations and retry behaviors.
 * Uses atomic counters with zero lock contention, ready for Micrometer in Phase 4.
 */
public class DlqMetrics {
    private final AtomicLong totalRouted = new AtomicLong(0);
    private final AtomicLong retryAttempts = new AtomicLong(0);
    private final AtomicLong produceFailures = new AtomicLong(0);
    private final AtomicLong fastFailCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> perTopicRouted = new ConcurrentHashMap<>();

    public void recordRouted(String topic) {
        totalRouted.incrementAndGet();
        perTopicRouted.computeIfAbsent(topic, t -> new AtomicLong(0)).incrementAndGet();
    }

    public void recordRetryAttempt() {
        retryAttempts.incrementAndGet();
    }

    public void recordProduceFailure() {
        produceFailures.incrementAndGet();
    }

    public void recordFastFail() {
        fastFailCount.incrementAndGet();
    }

    public long getTotalRouted() {
        return totalRouted.get();
    }

    public long getRetryAttempts() {
        return retryAttempts.get();
    }

    public long getProduceFailures() {
        return produceFailures.get();
    }

    public long getFastFailCount() {
        return fastFailCount.get();
    }

    public long getTopicRoutedCount(String topic) {
        AtomicLong c = perTopicRouted.get(topic);
        return c != null ? c.get() : 0;
    }

    public long getRoutedCount(String topic) {
        return getTopicRoutedCount(topic);
    }

    public void reset() {
        totalRouted.set(0);
        retryAttempts.set(0);
        produceFailures.set(0);
        fastFailCount.set(0);
        perTopicRouted.clear();
    }
}
