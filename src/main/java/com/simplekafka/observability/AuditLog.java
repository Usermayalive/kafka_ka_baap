package com.simplekafka.observability;

import com.google.gson.Gson;
import com.simplekafka.broker.Partition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lifecycle events appended as JSON to the internal topic {@code __audit} (one partition per
 * broker, local). Readable with any consumer. Audit writes never fail the operation being
 * audited; a failed write is logged.
 *
 * Record: {"ts":"...","broker":1,"event":"TOPIC_CREATED", ...details}
 */
public class AuditLog {
    private static final Logger LOGGER = Logger.getLogger(AuditLog.class.getName());
    public static final String TOPIC = "__audit";

    private static final Gson GSON = new Gson();
    private final int brokerId;
    private final Supplier<Partition> partition;

    public AuditLog(int brokerId, Supplier<Partition> partition) {
        this.brokerId = brokerId;
        this.partition = partition;
    }

    public void record(String event, Map<String, ?> details) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("broker", brokerId);
        entry.put("event", event);
        entry.putAll(details);
        try {
            partition.get().append(GSON.toJson(entry).getBytes(StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "Failed to write audit event " + event, e);
        }
    }
}
