package com.simplekafka.network;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * Standard length-prefixed protocol frame for SimpleKafka Netty networking layer.
 *
 * Wire format:
 * [Length: 4B (handled by length framer)]
 * [ApiKey: 2B]
 * [ApiVersion: 2B]
 * [CorrelationId: 4B]
 * [Payload: remaining bytes]
 */
public class KafkaFrame {
    // API Keys
    public static final short API_KEY_PRODUCE = 1;
    public static final short API_KEY_FETCH = 2;
    public static final short API_KEY_INIT_PRODUCER_ID = 3;
    public static final short API_KEY_METADATA = 4;
    public static final short API_KEY_CREATE_TOPIC = 5;
    public static final short API_KEY_FIND_COORDINATOR = 6;
    public static final short API_KEY_OFFSET_COMMIT = 7;
    public static final short API_KEY_OFFSET_FETCH = 8;
    public static final short API_KEY_JOIN_GROUP = 9;
    public static final short API_KEY_SYNC_GROUP = 10;
    public static final short API_KEY_HEARTBEAT = 11;
    public static final short API_KEY_LEAVE_GROUP = 12;
    public static final short API_KEY_REGISTER_SCHEMA = 13;
    public static final short API_KEY_SET_SCHEMA_COMPATIBILITY = 14;
    public static final short API_KEY_GET_SCHEMA = 15;
    public static final short API_KEY_FORGET_ENTITY = 16;
    public static final short API_KEY_REPLICA_FETCH = 17;
    public static final short API_KEY_PRODUCE_BATCH = 18;

    // Error Codes
    public static final short ERROR_NONE = 0;
    public static final short ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short ERROR_PRODUCER_FENCED = 1001;
    public static final short ERROR_OUT_OF_ORDER_SEQUENCE = 1002;
    public static final short ERROR_QUOTA_EXCEEDED = 1003;
    public static final short ERROR_CORRUPT_MESSAGE = 1004;
    public static final short ERROR_SERVER_ERROR = 1005;
    public static final short ERROR_BROKER_OVERLOADED = 1006;
    public static final short ERROR_DUPLICATE_SEQUENCE = 1007;
    public static final short ERROR_NOT_CONTROLLER = 1008;
    public static final short ERROR_NOT_COORDINATOR = 1009;
    public static final short ERROR_COORDINATOR_NOT_AVAILABLE = 1010;
    public static final short ERROR_UNKNOWN_MEMBER = 1011;
    public static final short ERROR_NOT_LEADER = 1012;
    public static final short ERROR_LEADER_NOT_AVAILABLE = 1013;
    public static final short ERROR_SCHEMA_VIOLATION = 1014;
    public static final short ERROR_INCOMPATIBLE_SCHEMA = 1015;
    public static final short ERROR_SCHEMA_NOT_FOUND = 1016;
    public static final short ERROR_NOT_ENOUGH_REPLICAS = 1017;
    public static final short ERROR_REQUEST_TIMED_OUT = 1018;

    /** Produce acks: leader-only (written locally) or all in-sync replicas. */
    public static final byte ACKS_LEADER = 1;
    public static final byte ACKS_ALL = -1;

    /** Produce with this producer ID to bypass idempotence (Kafka's NO_PRODUCER_ID). */
    public static final long NO_PRODUCER_ID = -1L;

    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final byte[] payload;

    public KafkaFrame(short apiKey, short apiVersion, int correlationId, byte[] payload) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.payload = payload != null ? payload : new byte[0];
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void encode(ByteBuf out) {
        out.writeShort(apiKey);
        out.writeShort(apiVersion);
        out.writeInt(correlationId);
        out.writeBytes(payload);
    }

    public static KafkaFrame decode(ByteBuf in) {
        short apiKey = in.readShort();
        short apiVersion = in.readShort();
        int correlationId = in.readInt();
        byte[] payload = new byte[in.readableBytes()];
        in.readBytes(payload);
        return new KafkaFrame(apiKey, apiVersion, correlationId, payload);
    }

    @Override
    public String toString() {
        return "KafkaFrame{" +
                "apiKey=" + apiKey +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", payloadBytes=" + payload.length +
                '}';
    }
}
