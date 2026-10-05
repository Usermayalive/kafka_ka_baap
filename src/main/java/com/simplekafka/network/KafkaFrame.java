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
