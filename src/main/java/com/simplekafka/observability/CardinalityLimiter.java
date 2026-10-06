package com.simplekafka.observability;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caps the number of distinct label values a metric may emit. The first {@code max} values are
 * admitted; later ones collapse into {@link #OVERFLOW}. Prometheus memory and query cost scale
 * with series count, so an unbounded label (a topic or group name chosen by clients) is a
 * denial-of-service vector for the monitoring stack, not just a tidiness issue.
 */
public class CardinalityLimiter {
    public static final String OVERFLOW = "__other__";

    private final int max;
    private final Set<String> admitted = ConcurrentHashMap.newKeySet();

    public CardinalityLimiter(int max) {
        this.max = max;
    }

    public String limit(String value) {
        if (admitted.contains(value)) {
            return value;
        }
        synchronized (admitted) {
            if (admitted.contains(value)) {
                return value;
            }
            if (admitted.size() < max) {
                admitted.add(value);
                return value;
            }
        }
        return OVERFLOW;
    }
}
