package com.simplekafka.client;

import com.simplekafka.broker.ProtocolException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Diagnostic envelope wrapping a poison-pill or failed message routed to a Dead Letter Queue (DLQ).
 * Captures comprehensive error context for forensic replay and telemetry.
 *
 * Serialization format:
 * [magic:2][version:1][origTopic:str][origPart:int][origOffset:long][reason:str][excClass:str][retryCount:int][timestamp:long]
 * [headerCount:short][(key:str, val:str)...][payloadLength:int][payloadBytes]
 */
public class DeadLetterEnvelope {
    public static final short MAGIC = 0x444C; // 'DL'
    public static final byte CURRENT_VERSION = 0x01;
    public static final int DLQ_MAX_MESSAGE_BYTES = 1024 * 1024; // 1 MB broker message limit
    public static final int MAX_ENVELOPE_OVERHEAD = 8 * 1024;    // 8 KB metadata headroom for headers & envelope
    public static final int MAX_PAYLOAD_BYTES = DLQ_MAX_MESSAGE_BYTES - MAX_ENVELOPE_OVERHEAD; // 1,040,384 bytes
    public static final int MAX_DEATH_REASON_BYTES = 4 * 1024;    // 4 KB max stack trace / death reason budget

    public static final String HEADER_DEATH_REASON = "X-Death-Reason";
    public static final String HEADER_EXCEPTION_CLASS = "X-Exception-Class";
    public static final String HEADER_ORIGINAL_TOPIC = "X-Original-Topic";
    public static final String HEADER_ORIGINAL_PARTITION = "X-Original-Partition";
    public static final String HEADER_ORIGINAL_OFFSET = "X-Original-Offset";
    public static final String HEADER_TIMESTAMP = "X-Timestamp";
    public static final String HEADER_RETRY_COUNT = "X-Retry-Count";
    public static final String HEADER_PAYLOAD_TRUNCATED = "X-Payload-Truncated";
    public static final String HEADER_ORIGINAL_SIZE = "X-Original-Size";
    public static final String HEADER_DEDUP_KEY = "X-Dedup-Key";

    private final byte version;
    private final String originalTopic;
    private final int originalPartition;
    private final long originalOffset;
    private final String deathReason;
    private final String exceptionClassName;
    private final int retryCount;
    private final long timestampMs;
    private final byte[] originalPayload;
    private final Map<String, String> headers;

    public DeadLetterEnvelope(String originalTopic,
                              int originalPartition,
                              long originalOffset,
                              Throwable cause,
                              int retryCount,
                              byte[] originalPayload) {
        this(originalTopic, originalPartition, originalOffset, cause, retryCount, originalPayload, Collections.emptyMap());
    }

    public DeadLetterEnvelope(String originalTopic,
                              int originalPartition,
                              long originalOffset,
                              Throwable cause,
                              int retryCount,
                              byte[] originalPayload,
                              Map<String, String> additionalHeaders) {
        this.version = CURRENT_VERSION;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;

        String rawReason = cause != null ? (cause.getMessage() != null ? cause.getMessage() : cause.toString()) : "Unknown error";
        this.deathReason = sanitizeAndTruncateDeathReason(rawReason);
        this.exceptionClassName = cause != null ? cause.getClass().getName() : "UnknownException";
        this.retryCount = retryCount;
        this.timestampMs = System.currentTimeMillis();

        Map<String, String> h = new HashMap<>();
        if (additionalHeaders != null) {
            h.putAll(additionalHeaders);
        }
        h.put(HEADER_ORIGINAL_TOPIC, originalTopic);
        h.put(HEADER_ORIGINAL_PARTITION, String.valueOf(originalPartition));
        h.put(HEADER_ORIGINAL_OFFSET, String.valueOf(originalOffset));
        h.put(HEADER_DEATH_REASON, deathReason);
        h.put(HEADER_EXCEPTION_CLASS, exceptionClassName);
        h.put(HEADER_RETRY_COUNT, String.valueOf(retryCount));
        h.put(HEADER_TIMESTAMP, String.valueOf(timestampMs));
        h.put(HEADER_DEDUP_KEY, originalTopic + ":" + originalPartition + ":" + originalOffset);

        // Dynamically calculate metadata overhead to compute effective maximum payload capacity
        int actualOverhead = computeOverhead(version, originalTopic, originalPartition, originalOffset,
                deathReason, exceptionClassName, retryCount, timestampMs, h);

        // Budget for potential truncation headers (X-Payload-Truncated and X-Original-Size)
        int truncationHeadersAllowance = (2 + utf8Len(HEADER_PAYLOAD_TRUNCATED)) + (2 + 4)
                + (2 + utf8Len(HEADER_ORIGINAL_SIZE)) + (2 + 12);

        int effectiveMaxPayload = DLQ_MAX_MESSAGE_BYTES - actualOverhead - truncationHeadersAllowance;
        int allowedPayloadCapacity = Math.min(MAX_PAYLOAD_BYTES, effectiveMaxPayload);

        if (originalPayload != null && originalPayload.length > allowedPayloadCapacity) {
            int safePayloadLen = Math.max(0, allowedPayloadCapacity);
            this.originalPayload = new byte[safePayloadLen];
            System.arraycopy(originalPayload, 0, this.originalPayload, 0, safePayloadLen);
            h.put(HEADER_PAYLOAD_TRUNCATED, "true");
            h.put(HEADER_ORIGINAL_SIZE, String.valueOf(originalPayload.length));
        } else {
            this.originalPayload = originalPayload != null ? originalPayload : new byte[0];
        }
        this.headers = Collections.unmodifiableMap(h);
    }

    public DeadLetterEnvelope(String originalTopic,
                              int originalPartition,
                              long originalOffset,
                              String deathReason,
                              String exceptionClassName,
                              int retryCount,
                              long timestampMs,
                              byte[] originalPayload,
                              Map<String, String> headers) {
        this(CURRENT_VERSION, originalTopic, originalPartition, originalOffset, deathReason, exceptionClassName, retryCount, timestampMs, originalPayload, headers);
    }

    public DeadLetterEnvelope(byte version,
                              String originalTopic,
                              int originalPartition,
                              long originalOffset,
                              String deathReason,
                              String exceptionClassName,
                              int retryCount,
                              long timestampMs,
                              byte[] originalPayload,
                              Map<String, String> headers) {
        this.version = version;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.deathReason = sanitizeAndTruncateDeathReason(deathReason);
        this.exceptionClassName = exceptionClassName;
        this.retryCount = retryCount;
        this.timestampMs = timestampMs;
        this.originalPayload = originalPayload != null ? originalPayload : new byte[0];
        this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
    }

    private static String sanitizeAndTruncateDeathReason(String reason) {
        if (reason == null) return "Unknown error";
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_DEATH_REASON_BYTES) {
            return reason;
        }
        // Truncate to MAX_DEATH_REASON_BYTES while preserving valid UTF-8
        int maxLen = Math.min(reason.length(), MAX_DEATH_REASON_BYTES - 32);
        return reason.substring(0, maxLen) + "... [truncated]";
    }

    private static int utf8Len(String s) {
        if (s == null) return 0;
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int computeOverhead(byte version, String topic, int partition, long offset,
                                       String reason, String excClass, int retries, long timestamp,
                                       Map<String, String> headers) {
        // [magic:2][version:1][origTopic:str][origPart:4][origOffset:8][reason:str][excClass:str][retryCount:4][timestamp:8]
        // [headerCount:2][(k,v)...][payloadLength:4]
        int len = 2 + 1;
        len += 2 + utf8Len(topic);
        len += 4; // partition
        len += 8; // offset
        len += 2 + utf8Len(reason);
        len += 2 + utf8Len(excClass);
        len += 4; // retryCount
        len += 8; // timestamp
        len += 2; // headerCount
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                len += 2 + utf8Len(entry.getKey());
                len += 2 + utf8Len(entry.getValue());
            }
        }
        len += 4; // payloadLength
        return len;
    }

    /**
     * Serializes this envelope into a binary wire format.
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeShort(MAGIC); // 'DL' magic identifier
            dos.writeByte(version); // format version
            dos.writeUTF(originalTopic);
            dos.writeInt(originalPartition);
            dos.writeLong(originalOffset);
            dos.writeUTF(deathReason);
            dos.writeUTF(exceptionClassName);
            dos.writeInt(retryCount);
            dos.writeLong(timestampMs);

            dos.writeShort(headers.size());
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }

            dos.writeInt(originalPayload.length);
            dos.write(originalPayload);
            dos.flush();
        }
        return baos.toByteArray();
    }

    public static DeadLetterEnvelope deserialize(byte[] data) throws IOException {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            short magic = dis.readShort();
            if (magic != MAGIC) {
                throw new ProtocolException("Invalid DeadLetterEnvelope magic header: 0x" + Integer.toHexString(magic & 0xFFFF));
            }
            byte ver = dis.readByte();
            if (ver != CURRENT_VERSION) {
                throw new ProtocolException("Unsupported DeadLetterEnvelope wire version: 0x" + Integer.toHexString(ver & 0xFF) + " (expected: 0x" + Integer.toHexString(CURRENT_VERSION & 0xFF) + ")");
            }

            String topic = dis.readUTF();
            int partition = dis.readInt();
            long offset = dis.readLong();
            String reason = dis.readUTF();
            String excClass = dis.readUTF();
            int retries = dis.readInt();
            long timestamp = dis.readLong();

            short headerCount = dis.readShort();
            Map<String, String> headers = new HashMap<>();
            for (int i = 0; i < headerCount; i++) {
                headers.put(dis.readUTF(), dis.readUTF());
            }

            int payloadLen = dis.readInt();
            byte[] payload = new byte[payloadLen];
            dis.readFully(payload);

            return new DeadLetterEnvelope(topic, partition, offset, reason, excClass, retries, timestamp, payload, headers);
        }
    }

    public String getDeduplicationKey() {
        return originalTopic + ":" + originalPartition + ":" + originalOffset;
    }

    public byte getVersion() {
        return version;
    }

    public String getOriginalTopic() {
        return originalTopic;
    }

    public int getOriginalPartition() {
        return originalPartition;
    }

    public long getOriginalOffset() {
        return originalOffset;
    }

    public String getDeathReason() {
        return deathReason;
    }

    public String getExceptionClassName() {
        return exceptionClassName;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public byte[] getOriginalPayload() {
        return originalPayload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return "DeadLetterEnvelope{" +
                "originalTopic='" + originalTopic + '\'' +
                ", originalPartition=" + originalPartition +
                ", originalOffset=" + originalOffset +
                ", deathReason='" + deathReason + '\'' +
                ", retryCount=" + retryCount +
                ", payloadLen=" + originalPayload.length +
                '}';
    }
}
