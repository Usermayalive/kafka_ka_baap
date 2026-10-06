package com.simplekafka.gdpr;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS KMS provider with one customer master key (CMK) per entity. Each DEK is wrapped by its
 * entity's CMK; forgetting an entity schedules that CMK for deletion, which makes every copy of
 * the wrapped DEK permanently unwrappable, including copies in backups and RocksDB files that
 * compaction hasn't purged yet. That guarantee is the reason for paying per key.
 *
 * Aliases are {@code alias/<prefix>/<sha256(entityId)>} (entity IDs may contain characters
 * alias names don't allow, and hashing avoids putting personal data in key metadata).
 */
public class KmsMasterKeyProvider implements MasterKeyProvider {
    /** KMS minimum; the key is unusable (Decrypt fails) as soon as deletion is scheduled. */
    public static final int PENDING_DELETION_DAYS = 7;

    private final KmsClient kms;
    private final String aliasPrefix;
    private final Map<String, String> keyIds = new ConcurrentHashMap<>();

    public KmsMasterKeyProvider(KmsClient kms, String aliasPrefix) {
        this.kms = kms;
        this.aliasPrefix = aliasPrefix;
    }

    String aliasFor(String entityId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(entityId.getBytes(StandardCharsets.UTF_8));
            return "alias/" + aliasPrefix + "/" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private String keyIdFor(String entityId, boolean create) throws IOException {
        String cached = keyIds.get(entityId);
        if (cached != null) {
            return cached;
        }
        String alias = aliasFor(entityId);
        try {
            String keyId = kms.describeKey(r -> r.keyId(alias)).keyMetadata().keyId();
            keyIds.put(entityId, keyId);
            return keyId;
        } catch (NotFoundException e) {
            if (!create) {
                throw new KeyDestroyedException("No KMS key for entity (forgotten or never created)");
            }
        }
        synchronized (this) {
            String keyId = kms.createKey(r -> r.description("StormLog entity data key wrapper")
                    .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)).keyMetadata().keyId();
            kms.createAlias(r -> r.aliasName(alias).targetKeyId(keyId));
            keyIds.put(entityId, keyId);
            return keyId;
        }
    }

    @Override
    public byte[] wrap(String entityId, byte[] dek) throws IOException {
        String keyId = keyIdFor(entityId, true);
        return kms.encrypt(r -> r.keyId(keyId).plaintext(SdkBytes.fromByteArray(dek))
                .encryptionContext(Map.of("stormlog-entity", aliasFor(entityId)))).ciphertextBlob().asByteArray();
    }

    @Override
    public byte[] unwrap(String entityId, byte[] wrappedDek) throws IOException {
        String keyId = keyIdFor(entityId, false);
        try {
            return kms.decrypt(r -> r.keyId(keyId).ciphertextBlob(SdkBytes.fromByteArray(wrappedDek))
                    .encryptionContext(Map.of("stormlog-entity", aliasFor(entityId)))).plaintext().asByteArray();
        } catch (KmsInvalidStateException | DisabledException e) {
            throw new KeyDestroyedException("KMS key for entity is pending deletion");
        }
    }

    @Override
    public void onEntityForgotten(String entityId) throws IOException {
        String keyId;
        try {
            keyId = keyIdFor(entityId, false);
        } catch (KeyDestroyedException e) {
            return; // never had a key, or already forgotten
        }
        kms.scheduleKeyDeletion(r -> r.keyId(keyId).pendingWindowInDays(PENDING_DELETION_DAYS));
        kms.deleteAlias(r -> r.aliasName(aliasFor(entityId)));
        keyIds.remove(entityId);
    }

    @Override
    public void close() {
        kms.close();
    }
}
