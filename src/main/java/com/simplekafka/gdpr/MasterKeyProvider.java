package com.simplekafka.gdpr;

import java.io.IOException;

/**
 * Wraps and unwraps per-entity data encryption keys (DEKs). DEKs are only ever stored wrapped.
 * See docs/gdpr-threat-model.md for what each implementation protects against.
 */
public interface MasterKeyProvider extends AutoCloseable {

    byte[] wrap(String entityId, byte[] dek) throws IOException;

    /** @throws KeyDestroyedException if the entity's key material no longer exists */
    byte[] unwrap(String entityId, byte[] wrappedDek) throws IOException;

    /** Called after the entity's wrapped DEK is deleted; providers may destroy key material too. */
    void onEntityForgotten(String entityId) throws IOException;

    @Override
    default void close() {
    }

    class KeyDestroyedException extends IOException {
        public KeyDestroyedException(String message) {
            super(message);
        }
    }
}
