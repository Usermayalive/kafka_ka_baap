package com.simplekafka.broker;

import java.io.IOException;

/**
 * Thrown when a wire protocol violation occurs (e.g., magic header mismatch,
 * unsupported protocol version, or corrupt framing), distinct from general I/O transport failures.
 */
public class ProtocolException extends IOException {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
