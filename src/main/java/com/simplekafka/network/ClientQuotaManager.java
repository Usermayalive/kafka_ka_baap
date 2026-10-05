package com.simplekafka.network;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Token-Bucket Rate Limiter policing per-client byte rate and request rate quotas.
 * Prevents noisy-neighbor starvation and broker saturation.
 *
 * NOTE: clientId is currently self-declared in protocol frames and spoofable in non-TLS environments.
 * Production hardening requires mTLS with client certificate CN/SAN mapping or SASL/SCRAM authentication.
 */
public class ClientQuotaManager {
    public static final long DEFAULT_MAX_BYTES_PER_SEC = 50 * 1024 * 1024; // 50 MB/s
    public static final long DEFAULT_MAX_REQUESTS_PER_SEC = 5_000;          // 5,000 req/s

    private final long maxBytesPerSecond;
    private final long maxRequestsPerSecond;

    private static class ClientBucket {
        double byteTokens;
        double requestTokens;
        long lastRefillNanos;

        ClientBucket(double byteTokens, double requestTokens, long nowNanos) {
            this.byteTokens = byteTokens;
            this.requestTokens = requestTokens;
            this.lastRefillNanos = nowNanos;
        }
    }

    private final ConcurrentHashMap<String, ClientBucket> buckets = new ConcurrentHashMap<>();

    public ClientQuotaManager() {
        this(DEFAULT_MAX_BYTES_PER_SEC, DEFAULT_MAX_REQUESTS_PER_SEC);
    }

    public ClientQuotaManager(long maxBytesPerSecond, long maxRequestsPerSecond) {
        this.maxBytesPerSecond = maxBytesPerSecond;
        this.maxRequestsPerSecond = maxRequestsPerSecond;
    }

    public boolean tryConsume(String clientId, int bytesRequested) {
        try {
            checkAndConsume(clientId, bytesRequested);
            return true;
        } catch (QuotaExceededException e) {
            return false;
        }
    }

    public void checkAndConsume(String clientId, int bytesRequested) throws QuotaExceededException {
        long nowNanos = System.nanoTime();
        ClientBucket bucket = buckets.computeIfAbsent(clientId,
                k -> new ClientBucket(maxBytesPerSecond, maxRequestsPerSecond, nowNanos));

        synchronized (bucket) {
            long elapsedNanos = nowNanos - bucket.lastRefillNanos;
            if (elapsedNanos > 0) {
                double elapsedSec = (double) elapsedNanos / TimeUnit.SECONDS.toNanos(1);
                bucket.byteTokens = Math.min(maxBytesPerSecond, bucket.byteTokens + elapsedSec * maxBytesPerSecond);
                bucket.requestTokens = Math.min(maxRequestsPerSecond, bucket.requestTokens + elapsedSec * maxRequestsPerSecond);
                bucket.lastRefillNanos = nowNanos;
            }

            if (bucket.requestTokens < 1.0) {
                long throttleMs = (long) Math.ceil((1.0 - bucket.requestTokens) / maxRequestsPerSecond * 1000);
                throw new QuotaExceededException(clientId, throttleMs,
                        String.format("Client [%s] exceeded request rate quota (%d req/s). Throttle: %d ms",
                                clientId, maxRequestsPerSecond, throttleMs));
            }

            if (bucket.byteTokens < bytesRequested) {
                long throttleMs = (long) Math.ceil((bytesRequested - bucket.byteTokens) / maxBytesPerSecond * 1000);
                throw new QuotaExceededException(clientId, throttleMs,
                        String.format("Client [%s] exceeded byte rate quota (%d B/s). Requested: %d B, Throttle: %d ms",
                                clientId, maxBytesPerSecond, bytesRequested, throttleMs));
            }

            bucket.requestTokens -= 1.0;
            bucket.byteTokens -= bytesRequested;
        }
    }

    public void reset() {
        buckets.clear();
    }
}
