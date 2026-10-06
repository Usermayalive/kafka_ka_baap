package com.simplekafka.tiered;

/**
 * @param remote                      where sealed segments are copied
 * @param localRetainedSealedSegments sealed segments kept on local disk after upload
 *                                    (newest first); -1 keeps all local copies
 * @param cachedRemoteSegments        downloaded remote segments kept for reads (LRU)
 */
public record TieredStorageConfig(RemoteStorageManager remote, int localRetainedSealedSegments, int cachedRemoteSegments) {
    public TieredStorageConfig {
        if (cachedRemoteSegments < 1) {
            throw new IllegalArgumentException("cachedRemoteSegments must be >= 1");
        }
    }
}
