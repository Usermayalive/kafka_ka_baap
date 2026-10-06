package com.simplekafka.metadata;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Applies committed {@link MetadataCommand}s to a {@link MetadataImage}, snapshots the image to
 * a single JSON file, and restores it on restart (Ratis then replays log entries after the
 * snapshot index).
 *
 * Read-only queries carry broker heartbeats: a query is served by the leader, which records
 * the broker's last-seen time in leader-local (unreplicated) state. Liveness is soft state, as
 * in KRaft: replicating every heartbeat would flood the log.
 */
public class MetadataStateMachine extends BaseStateMachine {
    private static final Logger LOGGER = Logger.getLogger(MetadataStateMachine.class.getName());

    private final MetadataImage image = new MetadataImage();
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final List<Consumer<MetadataImage.Change>> changeListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<RaftPeerId>> leaderListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<MetadataCommand>> commandListeners = new CopyOnWriteArrayList<>();
    /** Leader-local: broker id -> last heartbeat (nanoTime). */
    private final Map<Integer, Long> lastHeartbeat = new ConcurrentHashMap<>();
    // Listeners run off the apply thread, in commit order
    private final ExecutorService notifier = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "metadata-notifier");
        t.setDaemon(true);
        return t;
    });

    public MetadataImage image() {
        return image;
    }

    public void onChange(Consumer<MetadataImage.Change> listener) {
        changeListeners.add(listener);
    }

    /** Called (off the apply thread, in commit order) with every successfully applied command. */
    public void onCommand(Consumer<MetadataCommand> listener) {
        commandListeners.add(listener);
    }

    public void onLeaderChange(Consumer<RaftPeerId> listener) {
        leaderListeners.add(listener);
    }

    public Map<Integer, Long> lastHeartbeats() {
        return lastHeartbeat;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        loadSnapshot(storage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException {
        loadSnapshot(storage.loadLatestSnapshot());
    }

    @Override
    public SimpleStateMachineStorage getStateMachineStorage() {
        return storage;
    }

    private void loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            return;
        }
        Path file = snapshot.getFile().getPath();
        image.loadJson(Files.readString(file, StandardCharsets.UTF_8));
        setLastAppliedTermIndex(snapshot.getTermIndex());
        LOGGER.info("Restored metadata snapshot at " + snapshot.getTermIndex() + " from " + file);
        notifyListeners(MetadataImage.Change.BROKERS);
        notifyListeners(MetadataImage.Change.TOPICS);
        notifyListeners(MetadataImage.Change.SCHEMAS);
    }

    @Override
    public long takeSnapshot() throws IOException {
        TermIndex last = getLastAppliedTermIndex();
        File file = storage.getSnapshotFile(last.getTerm(), last.getIndex());
        Path tmp = file.toPath().resolveSibling(file.getName() + ".tmp");
        Files.writeString(tmp, image.toJson(), StandardCharsets.UTF_8);
        Files.move(tmp, file.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        var md5 = MD5FileUtil.computeAndSaveMd5ForFile(file);
        storage.updateLatestSnapshot(new SingleFileSnapshotInfo(new FileInfo(file.toPath(), md5), last));
        return last.getIndex();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        RaftProtos.LogEntryProto entry = trx.getLogEntry();
        byte[] data = entry.getStateMachineLogEntry().getLogData().toByteArray();
        String reply;
        MetadataImage.Change change = MetadataImage.Change.NONE;
        try {
            MetadataCommand command = MetadataCommand.fromBytes(data);
            change = image.apply(command);
            reply = "OK";
            notifier.execute(() -> commandListeners.forEach(l -> safely(() -> l.accept(command))));
        } catch (IllegalStateException e) {
            reply = "ERR " + e.getMessage(); // deterministic rejection, identical on every node
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Undecodable metadata command at index " + entry.getIndex(), e);
            reply = "ERR undecodable command";
        }
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        notifyListeners(change);
        return CompletableFuture.completedFuture(Message.valueOf(reply));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        String q = request.getContent().toString(StandardCharsets.UTF_8);
        if (q.startsWith("HEARTBEAT ")) {
            lastHeartbeat.put(Integer.parseInt(q.substring("HEARTBEAT ".length()).trim()), System.nanoTime());
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        }
        if (q.equals("IMAGE")) {
            return CompletableFuture.completedFuture(Message.valueOf(image.toJson()));
        }
        return CompletableFuture.completedFuture(Message.valueOf("ERR unknown query"));
    }

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        // A new leader has no liveness history: give every registered broker a full timeout
        long now = System.nanoTime();
        image.brokers().keySet().forEach(id -> lastHeartbeat.put(id, now));
        notifier.execute(() -> leaderListeners.forEach(l -> safely(() -> l.accept(newLeaderId))));
    }

    private void notifyListeners(MetadataImage.Change change) {
        if (change == MetadataImage.Change.NONE) {
            return;
        }
        notifier.execute(() -> changeListeners.forEach(l -> safely(() -> l.accept(change))));
    }

    private static void safely(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Metadata listener failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        notifier.shutdown();
        super.close();
    }
}
