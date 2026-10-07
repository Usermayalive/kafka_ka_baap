# Crypto-shredding: design and threat model

A log is immutable by design, and GDPR Article 17 requires erasing a person's data on
request. Rewriting every segment (local, tiered to S3, in backups) for each erasure request
doesn't scale. Crypto-shredding gets around this: each person's personal data is encrypted
under that person's own key, and erasing the person means destroying the key. The ciphertext
can stay wherever it is, because nobody can read it any more.

## How it works

1. **Tagging.** An Avro schema marks personal fields with `"pii": true` and names the
   entity whose key protects them with a record-level `"pii.entity": "<field>"`. Registration
   rejects schemas where the entity field is missing or not a string, the entity field is itself
   tagged, or a PII field isn't a string. Ciphertext replaces the string value, so records stay
   valid under their schema.
2. **Produce.** The partition leader validates the record, then encrypts each PII field with
   AES-256-GCM under the entity's DEK (data encryption key). The format is
   `enc:v1:<generation>:base64(iv ‖ ciphertext ‖ tag)`, and the additional authenticated data is
   `entityId/field/generation`, so ciphertext can't be moved to another field, entity or key
   generation. Only ciphertext reaches the log, tiered storage and backups.
3. **Keys.** One random 256-bit DEK per entity and *key generation*, created on first use,
   wrapped by a `MasterKeyProvider`, and stored **in the Raft metadata** (`CREATE_DEK`, first
   proposal wins), so every replica of a partition, and any future leader, decrypts with the same
   key. Keys are named `<sha256(entityId)>-g<generation>`, so the raw entity ID never appears in
   the metadata, the key store or KMS. All brokers must share the master key provider. Standalone
   brokers (no quorum) keep wrapped keys in a local RocksDB store instead.
4. **Fetch.** The leader decrypts PII fields before returning records, so consumers see
   plaintext and need no key access.
5. **Erasure.** The broker receiving `FORGET_ENTITY` hashes the entity ID and replicates only
   the hash through Raft. The replicated metadata counts erasures per entity, and the count is
   the entity's current key generation. Every broker deletes the wrapped DEKs of older
   generations, compacts RocksDB, and calls the provider's `onEntityForgotten`. From then on the
   entity's existing PII reads as `[REDACTED]` on every broker, for every copy of the data.
   Data produced *after* the erasure (for example, the person signs up again) is encrypted under
   the new generation and stays readable: erasure covers what was held when it was requested.
   Erasure is audited (`ENTITY_FORGOTTEN`) under the hash only.
6. **Brokers that missed it.** The erasure counts are part of the metadata image and its
   snapshots. A broker catching up from a snapshot never sees the individual command, so on
   startup, and whenever an image is loaded, it reconciles: it destroys every key older than the
   replicated generation. Completed erasures are marked, so this is cheap to repeat. Decryption
   also checks the replicated generation directly, so data is redacted even before the local
   key is destroyed. Finally, a starting broker refuses produce and fetch until its own
   registration has been applied locally. Registration is appended after every earlier command,
   so a broker never serves before it has seen every earlier erasure.

## Master key providers

| Provider | Wrapping | What `forgetEntity` destroys |
|---|---|---|
| `EnvMasterKeyProvider` (dev) | One AES-256 master key from `STORMLOG_MASTER_KEY` | The wrapped DEK in RocksDB |
| `KmsMasterKeyProvider` | One AWS KMS key per entity (`alias/<prefix>/<sha256(entity)>`) | The wrapped DEK, **and** the entity's KMS key (`ScheduleKeyDeletion`, 7-day window; Decrypt fails immediately) |

Why pay for one KMS key per entity: with a shared master key, deleting the wrapped DEK only
removes the *current* copy. RocksDB deletes are tombstones until compaction, and backups or old
SST files may keep the wrapped DEK indefinitely. Anyone holding such a copy and the shared
master key can still unwrap the DEK. Deleting a per-entity KMS key makes every copy of that
wrapped DEK useless. The cost is about $1 per key per month plus API calls, so at large entity
counts a hierarchical scheme (e.g. per-tenant keys) is the realistic compromise.

## Threat model

| Adversary / event | Protected? | Why |
|---|---|---|
| Stolen disk, backup, or S3 bucket of log segments | ✅ | Segments contain only PII ciphertext; keys aren't in segments. |
| Read access to segments **and** the DEK store, but not the master key | ✅ | DEKs are stored wrapped. |
| Erasure request, then a stale backup of segments is restored | ✅ | The DEK is gone, so restored ciphertext is unreadable. |
| Erasure request, then a stale copy of the wrapped key (an old metadata snapshot, Raft log segment not yet purged, or backup) **plus** the env master key | ❌ (Env) / ✅ (KMS) | Erasure removes the wrapped key from the live image and future snapshots, but can't revoke copies already made; KMS key deletion can. |
| A malicious or compromised broker process | ❌ | The broker holds unwrapped DEKs in memory (LRU cache) and decrypts on fetch. Encryption at rest is not end-to-end encryption. |
| A consumer that already read plaintext before erasure | ❌ | Shredding can't recall copies made downstream; that's the consumer's own erasure obligation. |
| Non-PII fields, keys, topic names, offsets | ❌ | Only tagged fields are encrypted. Putting personal data in untagged fields or in the entity ID itself defeats the scheme; the entity ID should be an opaque surrogate key. |
| Traffic sniffing between client and broker | ❌ | No TLS in this project (non-goal); PII travels in plaintext on the wire. |
| Tampering with ciphertext in a segment | Detected | GCM authentication fails, and the field reads as `[REDACTED]` rather than as altered data. |

## Known limitations

- **Key placement follows the broker, not the entity.** Each broker keeps its own DEK for an
  entity, so the same person's records on two brokers use different keys. Erasure still works
  because the erasure state is replicated to all brokers and reconciled on startup.
- **Decryption on fetch costs CPU** (an Avro decode, AES-GCM and a re-encode per record on PII
  topics). Topics without PII fields are untouched.
- **Producers send plaintext PII to the broker.** Client-side encryption would remove the broker
  from the trust boundary at the cost of key distribution to every producer and consumer.
