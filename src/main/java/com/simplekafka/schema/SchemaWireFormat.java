package com.simplekafka.schema;

import java.nio.ByteBuffer;

/**
 * Record framing for topics with a registered schema:
 * {@code [magic:1][schema_id:4][payload_len:4][avro binary payload]}.
 *
 * The schema ID is the registry-assigned ID (unique across topics), so a consumer can decode
 * any record with one registry lookup, and the broker can reject a record framed with a schema
 * that was never registered for the topic.
 */
public final class SchemaWireFormat {
    public static final byte MAGIC = 0x53; // 'S'
    public static final int HEADER_BYTES = 1 + 4 + 4;

    public record Framed(int schemaId, byte[] payload) {}

    private SchemaWireFormat() {
    }

    public static byte[] frame(int schemaId, byte[] payload) {
        return ByteBuffer.allocate(HEADER_BYTES + payload.length)
                .put(MAGIC).putInt(schemaId).putInt(payload.length).put(payload).array();
    }

    public static boolean isFramed(byte[] record) {
        return record.length >= HEADER_BYTES && record[0] == MAGIC;
    }

    public static Framed parse(byte[] record) throws SchemaViolationException {
        if (record.length < HEADER_BYTES) {
            throw new SchemaViolationException("Record is " + record.length + " bytes; schema framing needs at least "
                    + HEADER_BYTES + " ([magic][schema_id][payload_len])");
        }
        ByteBuffer buf = ByteBuffer.wrap(record);
        byte magic = buf.get();
        if (magic != MAGIC) {
            throw new SchemaViolationException(String.format(
                    "Missing schema framing: magic byte 0x%02X, expected 0x%02X", magic, MAGIC));
        }
        int schemaId = buf.getInt();
        int length = buf.getInt();
        if (length != buf.remaining()) {
            throw new SchemaViolationException("Framing declares a " + length + "-byte payload but "
                    + buf.remaining() + " bytes follow");
        }
        byte[] payload = new byte[length];
        buf.get(payload);
        return new Framed(schemaId, payload);
    }
}
