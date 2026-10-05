package com.simplekafka.network;

import java.io.IOException;

/**
 * Thrown when a producer with a stale epoch attempts to produce to a partition,
 * fencing out zombie instances after a failover or restart.
 */
public class ProducerFencedException extends IOException {
    public ProducerFencedException(String message) {
        super(message);
    }
}
