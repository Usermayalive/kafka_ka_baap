package com.simplekafka.network;

import java.io.IOException;

/**
 * Thrown when an incoming sequence number skips the expected sequential counter,
 * detecting data loss or message gaps before records are appended.
 */
public class OutOfOrderSequenceException extends IOException {
    public OutOfOrderSequenceException(String message) {
        super(message);
    }
}
