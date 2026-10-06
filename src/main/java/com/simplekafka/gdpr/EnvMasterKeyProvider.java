package com.simplekafka.gdpr;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * Development provider: one AES-256 master key from an environment variable (base64, 32 bytes)
 * wraps every DEK. Erasure relies entirely on deleting the wrapped DEK; anyone holding an old
 * copy of the key store (backup, un-compacted RocksDB files) plus this master key can still
 * unwrap a forgotten entity's DEK. Use {@link KmsMasterKeyProvider} where that matters.
 */
public class EnvMasterKeyProvider implements MasterKeyProvider {
    public static final String ENV_VAR = "STORMLOG_MASTER_KEY";
    private final byte[] masterKey;

    public EnvMasterKeyProvider(byte[] masterKey) {
        if (masterKey.length != 32) {
            throw new IllegalArgumentException("Master key must be 32 bytes (AES-256), got " + masterKey.length);
        }
        this.masterKey = masterKey.clone();
    }

    public static EnvMasterKeyProvider fromEnvironment() {
        String encoded = System.getenv(ENV_VAR);
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalStateException(ENV_VAR + " is not set");
        }
        return new EnvMasterKeyProvider(Base64.getDecoder().decode(encoded.trim()));
    }

    @Override
    public byte[] wrap(String entityKey, byte[] dek) throws IOException {
        try {
            return AesGcm.encrypt(masterKey, dek, "dek:" + entityKey);
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to wrap DEK", e);
        }
    }

    @Override
    public byte[] unwrap(String entityKey, byte[] wrappedDek) throws IOException {
        try {
            return AesGcm.decrypt(masterKey, wrappedDek, "dek:" + entityKey);
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to unwrap DEK for entity (wrong master key or tampered store)", e);
        }
    }

    @Override
    public void onEntityForgotten(String entityKey) {
        // Nothing to destroy: the shared master key protects all other entities
    }
}
