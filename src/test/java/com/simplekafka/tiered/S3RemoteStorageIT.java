package com.simplekafka.tiered;

import com.simplekafka.broker.Partition;
import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Against a real S3-compatible server (MinIO, SeaweedFS, AWS). Enabled only when
 * STORMLOG_S3_ENDPOINT is set; see docker-compose.yml for a local server. Credentials come
 * from STORMLOG_S3_ACCESS_KEY / STORMLOG_S3_SECRET_KEY.
 */
@EnabledIfEnvironmentVariable(named = "STORMLOG_S3_ENDPOINT", matches = ".+")
public class S3RemoteStorageIT {

    @TempDir
    File tempDir;

    private static S3RemoteStorageManager manager(String bucket) {
        return S3RemoteStorageManager.create(System.getenv("STORMLOG_S3_ENDPOINT"), "us-east-1",
                System.getenv().getOrDefault("STORMLOG_S3_ACCESS_KEY", "stormlog"),
                System.getenv().getOrDefault("STORMLOG_S3_SECRET_KEY", "stormlog-secret"), bucket);
    }

    @Test
    void testSegmentsTierToS3AndColdReadsWork() throws Exception {
        String bucket = "stormlog-it-" + UUID.randomUUID().toString().substring(0, 8);
        try (S3RemoteStorageManager s3 = manager(bucket)) {
            s3.ensureBucket().get(30, TimeUnit.SECONDS);
            String topicDir = new File(tempDir, "orders").getAbsolutePath();
            Partition partition = new Partition(0, topicDir, DurabilityMode.OS_PAGE_CACHE,
                    new TieredStorageConfig(s3, 0, 2));
            for (int i = 0; i < 35; i++) {
                byte[] payload = new byte[100 * 1024];
                ByteBuffer.wrap(payload).putInt(i);
                partition.append(payload);
            }
            partition.awaitTieringIdle().get(60, TimeUnit.SECONDS);

            List<Long> remoteOnly = partition.remoteOnlySegments();
            assertFalse(remoteOnly.isEmpty());
            for (long base : remoteOnly) {
                assertTrue(s3.exists(new RemoteSegmentId("orders", 0, base)).get(30, TimeUnit.SECONDS),
                        "Segment " + base + " must be in the bucket");
            }

            // Restart: cold read from S3
            Partition restarted = new Partition(0, topicDir, DurabilityMode.OS_PAGE_CACHE,
                    new TieredStorageConfig(s3, 0, 2));
            assertEquals(7, ByteBuffer.wrap(restarted.readRecords(7, 1).get(0).payload()).getInt());

            for (long base : remoteOnly) {
                s3.deleteSegment(new RemoteSegmentId("orders", 0, base)).get(30, TimeUnit.SECONDS);
                assertFalse(s3.exists(new RemoteSegmentId("orders", 0, base)).get(30, TimeUnit.SECONDS));
            }
        }
    }
}
