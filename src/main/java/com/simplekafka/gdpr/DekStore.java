package com.simplekafka.gdpr;

import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Wrapped per-entity DEKs in RocksDB (entityId -> wrapped DEK). Values are ciphertext, so the
 * store is encrypted at rest under the master key provider. Deletes are followed by a range
 * compaction so the wrapped DEK doesn't linger in SST files longer than necessary (backups and
 * already-copied files are a different matter; see the threat model).
 */
public class DekStore implements KeyStore {
    static {
        RocksDB.loadLibrary();
    }

    private final Options options;
    private final RocksDB db;

    public DekStore(File directory) throws IOException {
        directory.mkdirs();
        options = new Options().setCreateIfMissing(true);
        try {
            db = RocksDB.open(options, directory.getAbsolutePath());
        } catch (RocksDBException e) {
            options.close();
            throw new IOException("Failed to open DEK store at " + directory, e);
        }
    }

    private static final String FORGOTTEN_PREFIX = "forgotten:";

    /** Marks an entity's erasure as fully applied (key deleted and provider notified). */
    public void markForgotten(String entityKey) throws IOException {
        put(FORGOTTEN_PREFIX + entityKey, new byte[]{1});
    }

    public boolean isMarkedForgotten(String entityKey) throws IOException {
        return get(FORGOTTEN_PREFIX + entityKey) != null;
    }

    public byte[] get(String entityId) throws IOException {
        try {
            return db.get(key(entityId));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public void put(String entityId, byte[] wrappedDek) throws IOException {
        try (WriteOptions sync = new WriteOptions().setSync(true)) {
            db.put(sync, key(entityId), wrappedDek);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(String keyName, byte[] wrappedDek) throws IOException {
        byte[] existing = get(keyName);
        if (existing != null) {
            return existing;
        }
        put(keyName, wrappedDek);
        return wrappedDek;
    }

    public void delete(String entityId) throws IOException {
        try (WriteOptions sync = new WriteOptions().setSync(true)) {
            db.delete(sync, key(entityId));
            db.flush(new FlushOptions().setWaitForFlush(true));
            db.compactRange(); // drop the tombstoned value from SST files now, not eventually
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private static byte[] key(String entityId) {
        return entityId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }
}
