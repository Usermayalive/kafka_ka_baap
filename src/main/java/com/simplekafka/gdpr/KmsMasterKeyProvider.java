package com.simplekafka.gdpr;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS KMS provider with one customer master key (CMK) per entity. Each DEK is wrapped by its
 * entity's CMK; forgetting an entity schedules that CMK for deletion, which makes every copy of
 * the wrapped DEK permanently unwrappable, including copies in backups and RocksDB files that
 * compaction hasn't purged yet. That guarantee is the reason for paying per key.
 *
 * Aliases are {@code alias/<prefix>/<entityKey>}; the entity key is a SHA-256, so entity IDs
 * (personal data, possibly with characters aliases don't allow) never appear in KMS metadata.
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

    String aliasFor(String entityKey) {
        return "alias/" + aliasPrefix + "/" + entityKey;
    }

    private String keyIdFor(String entityKey, boolean create) throws IOException {
        String cached = keyIds.get(entityKey);
        if (cached != null) {
            return cached;
        }
        String alias = aliasFor(entityKey);
        try {
            String keyId = kms.describeKey(r -> r.keyId(alias)).keyMetadata().keyId();
            keyIds.put(entityKey, keyId);
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
            keyIds.put(entityKey, keyId);
            return keyId;
        }
    }

    @Override
    public byte[] wrap(String entityKey, byte[] dek) throws IOException {
        String keyId = keyIdFor(entityKey, true);
        return kms.encrypt(r -> r.keyId(keyId).plaintext(SdkBytes.fromByteArray(dek))
                .encryptionContext(Map.of("stormlog-entity", aliasFor(entityKey)))).ciphertextBlob().asByteArray();
    }

    @Override
    public byte[] unwrap(String entityKey, byte[] wrappedDek) throws IOException {
        String keyId = keyIdFor(entityKey, false);
        try {
            return kms.decrypt(r -> r.keyId(keyId).ciphertextBlob(SdkBytes.fromByteArray(wrappedDek))
                    .encryptionContext(Map.of("stormlog-entity", aliasFor(entityKey)))).plaintext().asByteArray();
        } catch (KmsInvalidStateException | DisabledException e) {
            throw new KeyDestroyedException("KMS key for entity is pending deletion");
        }
    }

    @Override
    public void onEntityForgotten(String entityKey) throws IOException {
        String keyId;
        try {
            keyId = keyIdFor(entityKey, false);
        } catch (KeyDestroyedException e) {
            return; // never had a key, or already forgotten
        }
        kms.scheduleKeyDeletion(r -> r.keyId(keyId).pendingWindowInDays(PENDING_DELETION_DAYS));
        kms.deleteAlias(r -> r.aliasName(aliasFor(entityKey)));
        keyIds.remove(entityKey);
    }

    @Override
    public void close() {
        kms.close();
    }
}
