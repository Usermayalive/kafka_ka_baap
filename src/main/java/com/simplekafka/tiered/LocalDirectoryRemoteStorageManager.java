package com.simplekafka.tiered;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * "Remote" storage in a local directory (or a mounted shared filesystem). Same object layout
 * and compression as S3; used for tests and single-machine development.
 */
public class LocalDirectoryRemoteStorageManager implements RemoteStorageManager {
    private final Path root;
    private final ExecutorService io = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "remote-storage-local-io");
        t.setDaemon(true);
        return t;
    });

    public LocalDirectoryRemoteStorageManager(Path root) {
        this.root = root;
    }

    @Override
    public CompletableFuture<Void> copySegment(RemoteSegmentId id, Path logFile, Path indexFile) {
        return CompletableFuture.runAsync(() -> {
            try {
                put(id.logKey(), SegmentCodec.compress(Files.readAllBytes(logFile)));
                put(id.indexKey(), SegmentCodec.compress(Files.readAllBytes(indexFile)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, io);
    }

    @Override
    public CompletableFuture<Void> fetchSegment(RemoteSegmentId id, Path logDestination, Path indexDestination) {
        return CompletableFuture.runAsync(() -> {
            try {
                Files.write(logDestination, SegmentCodec.decompress(Files.readAllBytes(root.resolve(id.logKey()))));
                Files.write(indexDestination, SegmentCodec.decompress(Files.readAllBytes(root.resolve(id.indexKey()))));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, io);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(RemoteSegmentId id) {
        return CompletableFuture.runAsync(() -> {
            try {
                Files.deleteIfExists(root.resolve(id.logKey()));
                Files.deleteIfExists(root.resolve(id.indexKey()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, io);
    }

    public boolean exists(RemoteSegmentId id) {
        return Files.exists(root.resolve(id.logKey())) && Files.exists(root.resolve(id.indexKey()));
    }

    private void put(String key, byte[] bytes) throws IOException {
        Path target = root.resolve(key);
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(tmp, bytes);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void close() {
        io.shutdown();
    }
}
