package com.simplekafka.gdpr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 * Field-level encryption with one data key per entity (crypto-shredding): erasing an entity
 * means destroying its key, which makes every copy of its encrypted fields unreadable (local
 * segments, tiered S3 segments, backups) without rewriting the immutable log.
 *
 * <b>Key generations.</b> Erasure covers the data held when it was requested; data collected
 * afterwards (say, the person signs up again) is new processing. Each entity's key therefore has
 * a generation equal to the number of times it has been erased (replicated in the metadata
 * image). New data is encrypted under the current generation; ciphertext from an older
 * generation is always {@link #REDACTED}, whether or not this broker has destroyed that key yet.
 *
 * Ciphertext format: {@code enc:v1:<generation>:<base64(iv || ciphertext || tag)>}, with AAD
 * binding it to (entity, field, generation). Keys are stored and wrapped under
 * {@code <entityKey>-g<generation>}, where entityKey is the SHA-256 of the entity ID.
 */
public class CryptoShredder implements AutoCloseable {
    public static final String PREFIX = "enc:v1:";
    public static final String REDACTED = "[REDACTED]";
    private static final int DEK_CACHE_SIZE = 10_000;

    private final KeyStore store;
    private final MasterKeyProvider masterKeys;
    /** Replicated erasure count per entity key (0 = never erased). */
    private final ToIntFunction<String> erasureGeneration;
    private final Map<String, byte[]> dekCache = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > DEK_CACHE_SIZE;
        }
    };

    public CryptoShredder(KeyStore store, MasterKeyProvider masterKeys) {
        this(store, masterKeys, key -> 0);
    }

    public CryptoShredder(KeyStore store, MasterKeyProvider masterKeys, ToIntFunction<String> erasureGeneration) {
        this.store = store;
        this.masterKeys = masterKeys;
        this.erasureGeneration = erasureGeneration;
    }

    /** Hex SHA-256 of the entity ID: the only form in which entities are stored or replicated. */
    public static String entityKey(String entityId) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(entityId.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String keyName(String entityKey, int generation) {
        return entityKey + "-g" + generation;
    }

    public String encrypt(String entityId, String field, String plaintext) throws IOException {
        int generation = erasureGeneration.applyAsInt(entityKey(entityId));
        try {
            byte[] ciphertext = AesGcm.encrypt(dekFor(keyName(entityKey(entityId), generation), true),
                    plaintext.getBytes(StandardCharsets.UTF_8), aad(entityId, field, generation));
            return PREFIX + generation + ":" + Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IOException("Field encryption failed", e);
        }
    }

    /** Plaintext, or {@link #REDACTED} if the value was encrypted before an erasure of its entity. */
    public String decrypt(String entityId, String field, String value) throws IOException {
        if (value == null || !value.startsWith(PREFIX)) {
            return value;
        }
        int separator = value.indexOf(':', PREFIX.length());
        if (separator < 0) {
            return REDACTED;
        }
        int generation;
        try {
            generation = Integer.parseInt(value.substring(PREFIX.length(), separator));
        } catch (NumberFormatException e) {
            return REDACTED;
        }
        String key = entityKey(entityId);
        if (generation < erasureGeneration.applyAsInt(key)) {
            return REDACTED; // erased cluster-wide, even if this broker hasn't destroyed the key yet
        }
        byte[] dek = dekFor(keyName(key, generation), false);
        if (dek == null) {
            return REDACTED;
        }
        try {
            byte[] plaintext = AesGcm.decrypt(dek, Base64.getDecoder().decode(value.substring(separator + 1)),
                    aad(entityId, field, generation));
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return REDACTED; // tampered, or moved from another field/entity
        }
    }

    /**
     * Destroys every key of the entity older than {@code currentGeneration} (and, depending on
     * the provider, its master key material). Idempotent and cheap to repeat: completed
     * erasures are marked, so reconciliation only does work a crash interrupted.
     */
    public synchronized void forget(String entityKey, int currentGeneration) throws IOException {
        for (int generation = 0; generation < currentGeneration; generation++) {
            String name = keyName(entityKey, generation);
            if (store.isMarkedForgotten(name)) {
                continue;
            }
            synchronized (dekCache) {
                dekCache.remove(name);
            }
            store.delete(name);
            masterKeys.onEntityForgotten(name);
            store.markForgotten(name);
        }
    }

    private byte[] dekFor(String keyName, boolean create) throws IOException {
        synchronized (dekCache) {
            byte[] cached = dekCache.get(keyName);
            if (cached != null) {
                return cached;
            }
        }
        synchronized (this) {
            byte[] wrapped = store.get(keyName);
            byte[] dek;
            if (wrapped != null) {
                try {
                    dek = masterKeys.unwrap(keyName, wrapped);
                } catch (MasterKeyProvider.KeyDestroyedException e) {
                    return null;
                }
            } else if (create) {
                byte[] candidate = AesGcm.newKey();
                byte[] ours = masterKeys.wrap(keyName, candidate);
                byte[] stored = store.putIfAbsent(keyName, ours);
                // Another broker may have created this key first: everyone uses the stored one
                dek = stored == ours ? candidate : masterKeys.unwrap(keyName, stored);
            } else {
                return null;
            }
            synchronized (dekCache) {
                dekCache.put(keyName, dek);
            }
            return dek;
        }
    }

    private static String aad(String entityId, String field, int generation) {
        return entityId + "/" + field + "/g" + generation;
    }

    @Override
    public void close() {
        store.close();
        masterKeys.close();
    }

    /** Closes the key store but keeps the master key provider (when replacing the store). */
    public void closeStoreOnly() {
        store.close();
    }
}
