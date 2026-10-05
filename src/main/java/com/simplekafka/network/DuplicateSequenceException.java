package com.simplekafka.network;

import java.io.IOException;

/**
 * Thrown when an incoming sequence number has already been processed by the broker
 * but has fallen out of the deduplication sliding window (WINDOW_SIZE = 5).
 * Distinct from OutOfOrderSequenceException, which is thrown on sequence gaps.
 */
public class DuplicateSequenceException extends IOException {
    public DuplicateSequenceException(String message) {
        super(message);
    }
}
