package com.simplekafka.gdpr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

public class KmsMasterKeyProviderTest {

    @TempDir
    File tempDir;

    /** In-memory KMS: enough of the API for per-entity key lifecycle, including pending deletion. */
    static class FakeKms implements KmsClient {
        final Map<String, String> aliases = new ConcurrentHashMap<>();
        final Map<String, KeyState> keys = new ConcurrentHashMap<>();
        final Map<String, byte[]> blobs = new ConcurrentHashMap<>(); // ciphertext -> plaintext
        final Map<String, String> blobKey = new ConcurrentHashMap<>();
        final List<ScheduleKeyDeletionRequest> deletions = new ArrayList<>();

        @Override public String serviceName() { return "kms"; }
        @Override public void close() {}

        @Override
        public DescribeKeyResponse describeKey(DescribeKeyRequest r) {
            String id = aliases.getOrDefault(r.keyId(), keys.containsKey(r.keyId()) ? r.keyId() : null);
            if (id == null) {
                throw NotFoundException.builder().message("no such key").build();
            }
            return DescribeKeyResponse.builder().keyMetadata(KeyMetadata.builder().keyId(id).keyState(keys.get(id)).build()).build();
        }

        @Override
        public CreateKeyResponse createKey(CreateKeyRequest r) {
            String id = UUID.randomUUID().toString();
            keys.put(id, KeyState.ENABLED);
            return CreateKeyResponse.builder().keyMetadata(KeyMetadata.builder().keyId(id).build()).build();
        }

        @Override
        public CreateAliasResponse createAlias(CreateAliasRequest r) {
            aliases.put(r.aliasName(), r.targetKeyId());
            return CreateAliasResponse.builder().build();
        }

        @Override
        public DeleteAliasResponse deleteAlias(DeleteAliasRequest r) {
            aliases.remove(r.aliasName());
            return DeleteAliasResponse.builder().build();
        }

        @Override
        public EncryptResponse encrypt(EncryptRequest r) {
            String blob = UUID.randomUUID().toString();
            blobs.put(blob, r.plaintext().asByteArray());
            blobKey.put(blob, r.keyId());
            return EncryptResponse.builder().ciphertextBlob(SdkBytes.fromUtf8String(blob)).build();
        }

        @Override
        public DecryptResponse decrypt(DecryptRequest r) {
            String blob = r.ciphertextBlob().asUtf8String();
            if (keys.get(blobKey.get(blob)) != KeyState.ENABLED) {
                throw KmsInvalidStateException.builder().message("key pending deletion").build();
            }
            return DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(blobs.get(blob))).build();
        }

        @Override
        public ScheduleKeyDeletionResponse scheduleKeyDeletion(ScheduleKeyDeletionRequest r) {
            deletions.add(r);
            keys.put(r.keyId(), KeyState.PENDING_DELETION);
            return ScheduleKeyDeletionResponse.builder().build();
        }
    }

    @Test
    void testForgetEntitySchedulesKmsKeyDeletionAndRedacts() throws Exception {
        FakeKms kms = new FakeKms();
        KmsMasterKeyProvider provider = new KmsMasterKeyProvider(kms, "stormlog");
        try (CryptoShredder shredder = new CryptoShredder(new DekStore(new File(tempDir, "dek")), provider)) {
            String alice = shredder.encrypt("alice", "email", "alice@example.com");
            String bob = shredder.encrypt("bob", "email", "bob@example.com");
            assertEquals(2, kms.keys.size(), "One KMS key per entity");
            assertFalse(kms.aliases.keySet().stream().anyMatch(a -> a.contains("alice")),
                    "Aliases must not contain the entity ID");

            shredder.forget("alice");

            assertEquals(1, kms.deletions.size());
            assertEquals(KmsMasterKeyProvider.PENDING_DELETION_DAYS, kms.deletions.get(0).pendingWindowInDays());
            assertEquals(CryptoShredder.REDACTED, shredder.decrypt("alice", "email", alice));
            assertEquals("bob@example.com", shredder.decrypt("bob", "email", bob));
        }
    }

    @Test
    void testOldWrappedDekIsUselessOnceKmsKeyIsDeleted() throws Exception {
        // The threat KMS mode addresses: a stale copy of the wrapped DEK (backup, old SST file)
        FakeKms kms = new FakeKms();
        KmsMasterKeyProvider provider = new KmsMasterKeyProvider(kms, "stormlog");
        byte[] dek = new byte[32];
        byte[] wrapped = provider.wrap("alice", dek);
        assertArrayEquals(dek, provider.unwrap("alice", wrapped));

        provider.onEntityForgotten("alice");
        assertThrows(MasterKeyProvider.KeyDestroyedException.class, () -> provider.unwrap("alice", wrapped));
    }

    @Test
    void testCiphertextIsBoundToEntityAndField() throws Exception {
        byte[] master = new byte[32];
        try (CryptoShredder shredder = new CryptoShredder(new DekStore(new File(tempDir, "dek2")), new EnvMasterKeyProvider(master))) {
            String email = shredder.encrypt("alice", "email", "alice@example.com");
            assertEquals(CryptoShredder.REDACTED, shredder.decrypt("alice", "phone", email),
                    "Ciphertext moved to another field must not decrypt");
            assertEquals("alice@example.com", shredder.decrypt("alice", "email", email));
        }
    }
}
