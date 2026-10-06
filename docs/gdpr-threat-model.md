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
   `enc:v1:base64(iv ‖ ciphertext ‖ tag)`, and the additional authenticated data is
   `entityId/fieldName`, so ciphertext can't be moved to another field or another entity's
   record. Only ciphertext reaches the log, tiered storage and backups.
3. **Keys.** One random 256-bit DEK per entity, created on first use and stored *wrapped* by a
   `MasterKeyProvider` in a RocksDB store on each broker (`<data>/__dek_store`). The wrapped DEK
   is written with sync.
4. **Fetch.** The leader decrypts PII fields before returning records, so consumers see
   plaintext and need no key access.
5. **Erasure.** `FORGET_ENTITY` is a Raft metadata command, so **every** broker applies it.
   Each broker deletes the wrapped DEK, compacts RocksDB, and calls the provider's
   `onEntityForgotten`. From then on, the entity's PII fields read as `[REDACTED]` on every
   broker, for every copy of the data. Erasure is audited (`ENTITY_FORGOTTEN`) under a SHA-256 of
   the entity ID, so the audit trail doesn't re-record the identifier.

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
| Erasure request, then a stale backup of the DEK store **plus** the env master key | ❌ (Env) / ✅ (KMS) | The env provider can't revoke old wrapped copies; KMS key deletion can. |
| A malicious or compromised broker process | ❌ | The broker holds unwrapped DEKs in memory (LRU cache) and decrypts on fetch. Encryption at rest is not end-to-end encryption. |
| A consumer that already read plaintext before erasure | ❌ | Shredding can't recall copies made downstream; that's the consumer's own erasure obligation. |
| Non-PII fields, keys, topic names, offsets | ❌ | Only tagged fields are encrypted. Putting personal data in untagged fields or in the entity ID itself defeats the scheme; the entity ID should be an opaque surrogate key. |
| Traffic sniffing between client and broker | ❌ | No TLS in this project (non-goal); PII travels in plaintext on the wire. |
| Tampering with ciphertext in a segment | Detected | GCM authentication fails, and the field reads as `[REDACTED]` rather than as altered data. |

## Known limitations

- **Key placement follows the broker, not the entity.** Each broker keeps its own DEK for an
  entity, so the same person's records on two brokers use different keys. Erasure still works
  because `FORGET_ENTITY` is replicated to all brokers. A broker that is down during erasure
  applies the command from the Raft log when it returns, before serving reads.
- **Decryption on fetch costs CPU** (an Avro decode, AES-GCM and a re-encode per record on PII
  topics). Topics without PII fields are untouched.
- **Producers send plaintext PII to the broker.** Client-side encryption would remove the broker
  from the trust boundary at the cost of key distribution to every producer and consumer.
