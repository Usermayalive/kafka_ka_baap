package com.simplekafka.gdpr;

import com.simplekafka.metadata.MetadataCommand;
import com.simplekafka.metadata.MetadataImage;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Wrapped DEKs in the Raft metadata, so every replica of a partition (and therefore any future
 * leader) can decrypt it. First proposal for a key name wins; erasure removes the entity's
 * wrapped keys from the image in the same deterministic step that bumps its generation.
 * Erasure markers stay broker-local (they record local work, not cluster state).
 */
public class ReplicatedKeyStore implements KeyStore {

    @FunctionalInterface
    public interface Proposer {
        void submit(MetadataCommand command) throws IOException;
    }

    private final Supplier<MetadataImage> image;
    private final Proposer proposer;
    private final DekStore localMarkers;

    public ReplicatedKeyStore(Supplier<MetadataImage> image, Proposer proposer, DekStore localMarkers) {
        this.image = image;
        this.proposer = proposer;
        this.localMarkers = localMarkers;
    }

    @Override
    public byte[] get(String keyName) {
        return image.get().wrappedDek(keyName);
    }

    @Override
    public byte[] putIfAbsent(String keyName, byte[] wrappedDek) throws IOException {
        byte[] existing = get(keyName);
        if (existing != null) {
            return existing;
        }
        proposer.submit(MetadataCommand.createDek(keyName, wrappedDek)); // returns once applied locally
        byte[] winner = get(keyName);
        if (winner == null) {
            throw new IOException("Data key " + keyName + " not visible after commit (entity erased concurrently?)");
        }
        return winner;
    }

    @Override
    public void delete(String keyName) {
        // FORGET_ENTITY removes wrapped keys from the replicated image itself
    }

    @Override
    public void markForgotten(String keyName) throws IOException {
        localMarkers.markForgotten(keyName);
    }

    @Override
    public boolean isMarkedForgotten(String keyName) throws IOException {
        return localMarkers.isMarkedForgotten(keyName);
    }

    @Override
    public void close() {
        localMarkers.close();
    }
}
