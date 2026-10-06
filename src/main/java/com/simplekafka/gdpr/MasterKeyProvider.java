package com.simplekafka.gdpr;

import java.io.IOException;

/**
 * Wraps and unwraps per-entity data encryption keys (DEKs). DEKs are only ever stored wrapped.
 * See docs/gdpr-threat-model.md for what each implementation protects against.
 *
 * Entities are identified by their {@code entityKey}: the hex SHA-256 of the entity ID
 * ({@link CryptoShredder#entityKey}), so no raw identifier reaches key stores, KMS metadata,
 * the Raft log, or the audit trail.
 */
public interface MasterKeyProvider extends AutoCloseable {

    byte[] wrap(String entityKey, byte[] dek) throws IOException;

    /** @throws KeyDestroyedException if the entity's key material no longer exists */
    byte[] unwrap(String entityKey, byte[] wrappedDek) throws IOException;

    /** Called after the entity's wrapped DEK is deleted; providers may destroy key material too. */
    void onEntityForgotten(String entityKey) throws IOException;

    @Override
    default void close() {
    }

    class KeyDestroyedException extends IOException {
        public KeyDestroyedException(String message) {
            super(message);
        }
    }
}
