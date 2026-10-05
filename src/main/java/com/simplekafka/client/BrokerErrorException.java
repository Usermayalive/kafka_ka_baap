package com.simplekafka.client;

import java.io.IOException;

/** A broker answered a request with a non-zero KafkaFrame error code. */
public class BrokerErrorException extends IOException {
    private final short errorCode;

    public BrokerErrorException(short errorCode, String message) {
        super("Broker error " + errorCode + ": " + message);
        this.errorCode = errorCode;
    }

    public short getErrorCode() {
        return errorCode;
    }
}
