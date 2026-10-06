package com.simplekafka.tiered;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * SPI for moving sealed segments to and from remote storage (S3, MinIO, a shared filesystem).
 *
 * Every operation returns a future and must not block the calling thread on I/O: callers
 * include broker request threads and must never stall a Netty event loop. Implementations
 * compress segments (Zstd) on upload and decompress on fetch; the local on-disk format is
 * unchanged.
 */
public interface RemoteStorageManager extends AutoCloseable {

    /** Uploads a sealed segment's log and index. Completes once both objects are stored. */
    CompletableFuture<Void> copySegment(RemoteSegmentId id, Path logFile, Path indexFile);

    /** Downloads a segment into the given local files (overwriting them). */
    CompletableFuture<Void> fetchSegment(RemoteSegmentId id, Path logDestination, Path indexDestination);

    /** Deletes a segment's objects. Completes normally if they don't exist. */
    CompletableFuture<Void> deleteSegment(RemoteSegmentId id);

    @Override
    default void close() {
    }
}
