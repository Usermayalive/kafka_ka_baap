package com.simplekafka.network;

import java.io.IOException;

/**
 * Thrown when a client exceeds its configured byte-rate or request-rate quota,
 * protecting the broker from noisy-neighbor starvation.
 */
public class QuotaExceededException extends IOException {
    private final String clientId;
    private final long throttleTimeMs;

    public QuotaExceededException(String clientId, long throttleTimeMs, String message) {
        super(message);
        this.clientId = clientId;
        this.throttleTimeMs = throttleTimeMs;
    }

    public String getClientId() {
        return clientId;
    }

    public long getThrottleTimeMs() {
        return throttleTimeMs;
    }
}
