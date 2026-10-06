package com.simplekafka.gdpr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Field-level encryption with one data key per entity (crypto-shredding): erasing an entity
 * means destroying its key, which makes every copy of its encrypted fields unreadable (local
 * segments, tiered S3 segments, backups) without rewriting the immutable log.
 *
 * Ciphertext format: {@code enc:v1:<base64(iv || ciphertext || tag)>}, with AAD binding it to
 * (entity, field) so ciphertext can't be moved to another record's field.
 */
public class CryptoShredder implements AutoCloseable {
    public static final String PREFIX = "enc:v1:";
    public static final String REDACTED = "[REDACTED]";
    private static final int DEK_CACHE_SIZE = 10_000;

    private final DekStore store;
    private final MasterKeyProvider masterKeys;
    private final Map<String, byte[]> dekCache = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > DEK_CACHE_SIZE;
        }
    };

    public CryptoShredder(DekStore store, MasterKeyProvider masterKeys) {
        this.store = store;
        this.masterKeys = masterKeys;
    }

    public String encrypt(String entityId, String field, String plaintext) throws IOException {
        try {
            byte[] ciphertext = AesGcm.encrypt(dekFor(entityId, true), plaintext.getBytes(StandardCharsets.UTF_8),
                    aad(entityId, field));
            return PREFIX + Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IOException("Field encryption failed", e);
        }
    }

    /** Plaintext, or {@link #REDACTED} if the entity has been forgotten. */
    public String decrypt(String entityId, String field, String value) throws IOException {
        if (value == null || !value.startsWith(PREFIX)) {
            return value;
        }
        byte[] dek = dekFor(entityId, false);
        if (dek == null) {
            return REDACTED;
        }
        try {
            byte[] plaintext = AesGcm.decrypt(dek, Base64.getDecoder().decode(value.substring(PREFIX.length())),
                    aad(entityId, field));
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            // Written under an earlier key for this entity (forgotten, then re-created): still erased
            return REDACTED;
        }
    }

    /** Destroys the entity's DEK (and, depending on the provider, its master key material). */
    public synchronized void forget(String entityId) throws IOException {
        synchronized (dekCache) {
            dekCache.remove(entityId);
        }
        store.delete(entityId);
        masterKeys.onEntityForgotten(entityId);
    }

    private byte[] dekFor(String entityId, boolean create) throws IOException {
        synchronized (dekCache) {
            byte[] cached = dekCache.get(entityId);
            if (cached != null) {
                return cached;
            }
        }
        synchronized (this) {
            byte[] wrapped = store.get(entityId);
            byte[] dek;
            if (wrapped != null) {
                try {
                    dek = masterKeys.unwrap(entityId, wrapped);
                } catch (MasterKeyProvider.KeyDestroyedException e) {
                    return null;
                }
            } else if (create) {
                dek = AesGcm.newKey();
                store.put(entityId, masterKeys.wrap(entityId, dek));
            } else {
                return null;
            }
            synchronized (dekCache) {
                dekCache.put(entityId, dek);
            }
            return dek;
        }
    }

    private static String aad(String entityId, String field) {
        return entityId + "/" + field;
    }

    @Override
    public void close() {
        store.close();
        masterKeys.close();
    }
}
