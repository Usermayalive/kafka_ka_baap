package com.simplekafka.broker;

/**
 * Pluggable key extraction strategy for log compaction.
 * Decouples the storage engine from domain-specific envelope or message formats.
 */
@FunctionalInterface
public interface CompactionKeyExtractor {
    /**
     * Extracts the deduplication/compaction key from a raw stored message.
     * Return null or empty byte array to designate a tombstone.
     */
    String extractKey(byte[] message) throws Exception;
}
