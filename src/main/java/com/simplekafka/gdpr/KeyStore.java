package com.simplekafka.gdpr;

import java.io.IOException;

/** Where wrapped data keys live. Local (one broker) or replicated (every replica of the data). */
public interface KeyStore extends AutoCloseable {

    byte[] get(String keyName) throws IOException;

    /**
     * Stores {@code wrappedDek} unless the key already exists. Returns the stored value, which
     * is the caller's only if it won: concurrent creators on different brokers converge on one key.
     */
    byte[] putIfAbsent(String keyName, byte[] wrappedDek) throws IOException;

    void delete(String keyName) throws IOException;

    /** Local marker: this broker has fully applied the erasure of {@code keyName}. */
    void markForgotten(String keyName) throws IOException;

    boolean isMarkedForgotten(String keyName) throws IOException;

    @Override
    void close();
}
