package com.simplekafka.network;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Broker-side manager for allocating globally unique 64-bit Producer IDs (PIDs)
 * and monotonically incrementing epochs on producer recovery or reconnection.
 *
 * PID BIT LAYOUT (Kafka-style):
 * [BrokerId: 16 bits (63..48)] | [BrokerEpoch: 16 bits (47..32)] | [SequenceCounter: 32 bits (31..0)]
 *
 * GUARANTEES:
 * 1. Global Uniqueness Across Brokers: High 16 bits encode brokerId, eliminating multi-broker collisions.
 * 2. Uniqueness Across Broker Restarts: Middle 16 bits encode brokerEpoch (incremented and persisted on each start).
 * 3. Wall-Clock Independence: PIDs do not rely on System.currentTimeMillis().
 */
public class ProducerIdManager {
    private static final Logger LOGGER = Logger.getLogger(ProducerIdManager.class.getName());

    private final short brokerId;
    private final short brokerEpoch;
    private final AtomicInteger counter;
    private final ConcurrentHashMap<Long, Short> producerEpochs;
    private final File stateFile;

    public record ProducerIdAndEpoch(long producerId, short epoch) {}

    public ProducerIdManager() {
        this((short) 1, null);
    }

    public ProducerIdManager(short brokerId) {
        this(brokerId, null);
    }

    public ProducerIdManager(short brokerId, File stateFile) {
        this.brokerId = brokerId;
        this.stateFile = stateFile;
        this.producerEpochs = new ConcurrentHashMap<>();
        this.counter = new AtomicInteger(0);

        short epochToUse = 1;
        if (stateFile != null && stateFile.exists()) {
            epochToUse = loadAndIncrementEpoch();
        } else if (stateFile != null) {
            saveEpoch((short) 1);
        }
        this.brokerEpoch = epochToUse;
    }

    public synchronized ProducerIdAndEpoch generateProducerId() {
        int seq = counter.incrementAndGet();
        long pid = (((long) (brokerId & 0xFFFF)) << 48)
                | (((long) (brokerEpoch & 0xFFFF)) << 32)
                | ((long) seq & 0xFFFFFFFFL);

        producerEpochs.put(pid, (short) 0);
        return new ProducerIdAndEpoch(pid, (short) 0);
    }

    public synchronized ProducerIdAndEpoch bumpEpoch(long producerId) {
        short newEpoch = producerEpochs.compute(producerId, (k, old) -> (short) ((old == null ? 0 : old) + 1));
        return new ProducerIdAndEpoch(producerId, newEpoch);
    }

    public short getEpoch(long producerId) {
        return producerEpochs.getOrDefault(producerId, (short) 0);
    }

    public short getBrokerId() {
        return brokerId;
    }

    public short getBrokerEpoch() {
        return brokerEpoch;
    }

    private short loadAndIncrementEpoch() {
        short prevEpoch = 0;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(stateFile)))) {
            prevEpoch = dis.readShort();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read broker PID epoch state file, defaulting to epoch 1", e);
        }
        short nextEpoch = (short) (prevEpoch + 1);
        saveEpoch(nextEpoch);
        return nextEpoch;
    }

    private void saveEpoch(short epoch) {
        if (stateFile == null) return;
        File tmp = new File(stateFile.getParentFile(), stateFile.getName() + ".tmp");
        try {
            if (stateFile.getParentFile() != null) {
                stateFile.getParentFile().mkdirs();
            }
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmp)))) {
                dos.writeShort(epoch);
                dos.flush();
            }
            Files.move(tmp.toPath(), stateFile.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to persist broker PID epoch to " + stateFile, e);
        }
    }
}
