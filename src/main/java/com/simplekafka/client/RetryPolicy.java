package com.simplekafka.client;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configurable exponential backoff retry policy for consumer message execution.
 * Classifies exceptions into transient (retryable) vs fatal (fast-fail directly to DLQ).
 */
public class RetryPolicy {
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_MS = 100;
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    public static final long DEFAULT_MAX_BACKOFF_MS = 2000;

    /**
     * Default classifier:
     * - Fatal (skip retries -> direct to DLQ): IllegalArgumentException, NullPointerException,
     *   ClassCastException, Deserialization / Syntax / Schema errors.
     * - Retryable: Network timeouts, IOExceptions, database connectivity errors.
     */
    public static final Predicate<Throwable> DEFAULT_RETRYABLE_PREDICATE = t -> {
        if (t == null) return false;
        if (t instanceof IllegalArgumentException ||
            t instanceof NullPointerException ||
            t instanceof ClassCastException ||
            t instanceof IndexOutOfBoundsException) {
            return false;
        }
        String className = t.getClass().getName().toLowerCase();
        String message = t.getMessage() != null ? t.getMessage().toLowerCase() : "";
        if (className.contains("deserializ") || className.contains("syntax") || className.contains("schema") ||
            message.contains("deserializ") || message.contains("syntax") || message.contains("schema")) {
            return false;
        }
        return true;
    };

    private final int maxRetries;
    private final long initialBackoffMs;
    private final double backoffMultiplier;
    private final long maxBackoffMs;
    private final Predicate<Throwable> retryablePredicate;

    public RetryPolicy() {
        this(DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_BACKOFF_MULTIPLIER, DEFAULT_MAX_BACKOFF_MS, DEFAULT_RETRYABLE_PREDICATE);
    }

    public RetryPolicy(int maxRetries, long initialBackoffMs, double backoffMultiplier, long maxBackoffMs) {
        this(maxRetries, initialBackoffMs, backoffMultiplier, maxBackoffMs, DEFAULT_RETRYABLE_PREDICATE);
    }

    public RetryPolicy(int maxRetries, long initialBackoffMs, double backoffMultiplier, long maxBackoffMs, Predicate<Throwable> retryablePredicate) {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
        if (initialBackoffMs < 0) throw new IllegalArgumentException("initialBackoffMs must be >= 0");
        if (backoffMultiplier < 1.0) throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
        if (maxBackoffMs < initialBackoffMs) throw new IllegalArgumentException("maxBackoffMs must be >= initialBackoffMs");

        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMs = maxBackoffMs;
        this.retryablePredicate = Objects.requireNonNull(retryablePredicate, "retryablePredicate cannot be null");
    }

    public static RetryPolicy noRetries() {
        return new RetryPolicy(0, 0, 1.0, 0, t -> false);
    }

    public boolean isRetryable(Throwable t) {
        return retryablePredicate.test(t);
    }

    public RetryPolicy withRetryablePredicate(Predicate<Throwable> predicate) {
        return new RetryPolicy(this.maxRetries, this.initialBackoffMs, this.backoffMultiplier, this.maxBackoffMs, predicate);
    }

    /**
     * Calculates exponential backoff duration for a given attempt (1-based index).
     */
    public long calculateBackoffMs(int attempt) {
        if (attempt <= 1) {
            return initialBackoffMs;
        }
        double backoff = initialBackoffMs * Math.pow(backoffMultiplier, attempt - 1);
        return Math.min((long) backoff, maxBackoffMs);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public Predicate<Throwable> getRetryablePredicate() {
        return retryablePredicate;
    }
}
