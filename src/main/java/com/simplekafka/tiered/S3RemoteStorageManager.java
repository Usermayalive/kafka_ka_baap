package com.simplekafka.tiered;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * S3 (or any S3-compatible store such as MinIO) via the AWS SDK v2 async client, which runs
 * its own Netty event loops. Compression and file I/O run on a small dedicated pool; nothing
 * here blocks the caller.
 */
public class S3RemoteStorageManager implements RemoteStorageManager {
    private final S3AsyncClient s3;
    private final String bucket;
    private final ExecutorService codecPool = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "remote-storage-s3-codec");
        t.setDaemon(true);
        return t;
    });

    public S3RemoteStorageManager(S3AsyncClient s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    /**
     * Client for an S3-compatible endpoint with static credentials; {@code endpoint} may be null
     * for AWS itself. Path-style addressing is used for custom endpoints (MinIO requires it).
     */
    public static S3RemoteStorageManager create(String endpoint, String region, String accessKey, String secretKey,
                                                String bucket) {
        var builder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(32));
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
        }
        return new S3RemoteStorageManager(builder.build(), bucket);
    }

    /** Creates the bucket if missing. */
    public CompletableFuture<Void> ensureBucket() {
        return s3.headBucket(b -> b.bucket(bucket))
                .<Void>thenApply(r -> null)
                .exceptionallyCompose(e -> unwrap(e) instanceof NoSuchBucketException || is404(e)
                        ? s3.createBucket(b -> b.bucket(bucket)).thenApply(r -> null)
                        : CompletableFuture.failedFuture(unwrap(e)));
    }

    @Override
    public CompletableFuture<Void> copySegment(RemoteSegmentId id, Path logFile, Path indexFile) {
        // Index after log: a segment is only considered present when both objects exist.
        return compressFile(logFile)
                .thenCompose(bytes -> put(id.logKey(), bytes))
                .thenCompose(v -> compressFile(indexFile))
                .thenCompose(bytes -> put(id.indexKey(), bytes));
    }

    @Override
    public CompletableFuture<Void> fetchSegment(RemoteSegmentId id, Path logDestination, Path indexDestination) {
        return get(id.logKey())
                .thenAcceptAsync(bytes -> writeDecompressed(bytes, logDestination), codecPool)
                .thenCompose(v -> get(id.indexKey()))
                .thenAcceptAsync(bytes -> writeDecompressed(bytes, indexDestination), codecPool);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(RemoteSegmentId id) {
        return s3.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(
                        ObjectIdentifier.builder().key(id.logKey()).build(),
                        ObjectIdentifier.builder().key(id.indexKey()).build())))
                .thenApply(r -> null);
    }

    public CompletableFuture<Boolean> exists(RemoteSegmentId id) {
        return s3.headObject(b -> b.bucket(bucket).key(id.indexKey()))
                .thenApply(r -> true)
                .exceptionally(e -> {
                    if (unwrap(e) instanceof NoSuchKeyException || is404(e)) {
                        return false;
                    }
                    throw new CompletionException(unwrap(e));
                });
    }

    private CompletableFuture<byte[]> compressFile(Path file) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return SegmentCodec.compress(Files.readAllBytes(file));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, codecPool);
    }

    private static void writeDecompressed(byte[] compressed, Path destination) {
        try {
            Files.write(destination, SegmentCodec.decompress(compressed));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private CompletableFuture<Void> put(String key, byte[] body) {
        return s3.putObject(b -> b.bucket(bucket).key(key).contentLength((long) body.length)
                                .contentType("application/zstd"),
                        AsyncRequestBody.fromBytes(body))
                .thenApply(r -> null);
    }

    private CompletableFuture<byte[]> get(String key) {
        return s3.getObject(b -> b.bucket(bucket).key(key), AsyncResponseTransformer.toBytes())
                .thenApply(r -> r.asByteArray());
    }

    private static Throwable unwrap(Throwable e) {
        return e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
    }

    private static boolean is404(Throwable e) {
        return unwrap(e) instanceof S3Exception s3e && s3e.statusCode() == 404;
    }

    @Override
    public void close() {
        s3.close();
        codecPool.shutdown();
    }
}
