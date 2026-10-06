package com.simplekafka.metadata;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This node's member of the replicated metadata quorum (Apache Ratis over gRPC).
 *
 * Every broker embeds one (KRaft "combined mode"); the Raft leader acts as the controller.
 * Brokers prove liveness with heartbeats served by the leader; the leader unregisters brokers
 * silent for longer than the session timeout, which is how ZooKeeper's ephemeral nodes are
 * replaced.
 *
 * Membership: voters are fixed at startup; additional nodes join as non-voting listeners
 * (learners), catch up, and are promoted with {@link #promote}. Promotions that would leave an
 * even number of voters are refused: 4 voters tolerate no more failures than 3 but need a
 * larger majority.
 */
public class MetadataQuorum implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(MetadataQuorum.class.getName());

    public static final RaftGroupId GROUP_ID =
            RaftGroupId.valueOf(UUID.nameUUIDFromBytes("stormlog-metadata".getBytes(StandardCharsets.UTF_8)));
    public static final long DEFAULT_SESSION_TIMEOUT_MS = 3_000;
    private static final long HEARTBEAT_INTERVAL_MS = 500;
    /** A learner within this many entries of the leader's commit index counts as caught up. */
    private static final long CATCH_UP_SLACK = 5;

    /** "id@host:port" */
    public record Peer(int id, String host, int port) {
        public static Peer parse(String spec) {
            String[] idAndAddress = spec.trim().split("@", 2);
            String[] hostPort = idAndAddress[1].split(":", 2);
            return new Peer(Integer.parseInt(idAndAddress[0]), hostPort[0], Integer.parseInt(hostPort[1]));
        }

        public static List<Peer> parseList(String specs) {
            List<Peer> peers = new ArrayList<>();
            for (String spec : specs.split(",")) {
                if (!spec.isBlank()) {
                    peers.add(parse(spec));
                }
            }
            return peers;
        }

        RaftPeerId raftId() {
            return RaftPeerId.valueOf("n" + id);
        }

        RaftPeer toRaft(RaftProtos.RaftPeerRole role) {
            return RaftPeer.newBuilder().setId(raftId()).setAddress(host + ":" + port).setStartupRole(role).build();
        }
    }

    private final Peer self;
    private final List<Peer> voters;
    private final boolean joinAsListener;
    private final File storageDir;
    private final long sessionTimeoutMs;
    private final MetadataStateMachine stateMachine = new MetadataStateMachine();
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "metadata-quorum-timer");
        t.setDaemon(true);
        return t;
    });
    private final Set<Integer> fencingInFlight = ConcurrentHashMap.newKeySet();
    private RaftServer server;
    private RaftClient client;
    private volatile Integer heartbeatBrokerId;
    private long snapshotThreshold = 1_000;

    /**
     * @param voters         the initial voting members (including self unless joining as listener)
     * @param joinAsListener start as a non-voting learner of an existing quorum
     */
    public MetadataQuorum(Peer self, List<Peer> voters, boolean joinAsListener, File storageDir, long sessionTimeoutMs) {
        this.self = self;
        this.voters = List.copyOf(voters);
        this.joinAsListener = joinAsListener;
        this.storageDir = storageDir;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void start() throws IOException {
        RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, self.port());
        RaftServerConfigKeys.setStorageDir(properties, List.of(storageDir));
        // Sub-second elections: metadata failover should be fast on a LAN
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(400, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(800, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);

        List<RaftPeer> peers = new ArrayList<>();
        for (Peer v : voters) {
            peers.add(v.toRaft(RaftProtos.RaftPeerRole.FOLLOWER));
        }
        if (joinAsListener) {
            peers.add(self.toRaft(RaftProtos.RaftPeerRole.LISTENER));
        }
        RaftGroup group = RaftGroup.valueOf(GROUP_ID, peers);
        boolean existing = new File(storageDir, GROUP_ID.getUuid().toString()).exists();
        server = RaftServer.newBuilder()
                .setServerId(self.raftId())
                .setStateMachine(stateMachine)
                .setProperties(properties)
                .setGroup(group)
                .setOption(existing ? RaftStorage.StartupOption.RECOVER : RaftStorage.StartupOption.FORMAT)
                .build();
        server.start();
        client = RaftClient.newBuilder().setProperties(new RaftProperties()).setRaftGroup(group).build();

        timers.scheduleWithFixedDelay(this::heartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        timers.scheduleWithFixedDelay(this::fenceSilentBrokers, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        LOGGER.info("Metadata quorum member n" + self.id() + " started on " + self.host() + ":" + self.port()
                + (joinAsListener ? " as listener" : "") + (existing ? " (recovered)" : ""));
    }

    /** Log entries between automatic snapshots. Set before start(). */
    public void setSnapshotThreshold(long entries) {
        this.snapshotThreshold = entries;
    }

    public MetadataStateMachine stateMachine() {
        return stateMachine;
    }

    public MetadataImage image() {
        return stateMachine.image();
    }

    public Peer self() {
        return self;
    }

    /** Commits a command through the leader; throws IllegalStateException if the image rejected it. */
    public void submit(MetadataCommand command) throws IOException {
        RaftClientReply reply = client.io().send(Message.valueOf(ByteString.copyFrom(command.toBytes())));
        if (!reply.isSuccess()) {
            throw new IOException("Metadata command " + command.type + " failed", reply.getException());
        }
        String result = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
        if (result.startsWith("ERR ")) {
            throw new IllegalStateException(result.substring(4));
        }
        awaitLocallyApplied(reply.getLogIndex());
    }

    /**
     * Read-your-writes on this node: the reply comes back once the leader commits, but if this
     * node is a follower its own image may not include the entry yet.
     */
    private void awaitLocallyApplied(long logIndex) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (stateMachine.getLastAppliedTermIndex().getIndex() < logIndex) {
            if (System.nanoTime() > deadline) {
                throw new IOException("Committed metadata entry " + logIndex + " not applied locally within 10s");
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for metadata entry " + logIndex, e);
            }
        }
    }

    /**
     * Like {@link #submit} but gives up after {@code timeoutMs}: for best-effort writes such as
     * unregistering on shutdown, which can never commit once a majority of the quorum is gone.
     */
    public boolean trySubmit(MetadataCommand command, long timeoutMs) {
        try {
            RaftClientReply reply = client.async().send(Message.valueOf(ByteString.copyFrom(command.toBytes())))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return reply.isSuccess();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    /** Starts heartbeating on behalf of a registered broker. */
    public void heartbeatAs(int brokerId) {
        this.heartbeatBrokerId = brokerId;
    }

    private void heartbeat() {
        Integer id = heartbeatBrokerId;
        if (id == null) {
            return;
        }
        try {
            // Bounded: a blocking send can spend seconds retrying (e.g. right after a restart),
            // which would starve every later heartbeat on this timer thread
            client.async().sendReadOnly(Message.valueOf("HEARTBEAT " + id))
                    .get(HEARTBEAT_INTERVAL_MS * 2, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            LOGGER.log(Level.FINE, "Heartbeat to metadata leader failed; retrying next interval", e);
        }
    }

    /** Leader only: unregister brokers that stopped heartbeating (ZooKeeper ephemeral-node expiry). */
    private void fenceSilentBrokers() {
        if (!isLeader()) {
            return;
        }
        long now = System.nanoTime();
        for (MetadataImage.BrokerRegistration b : image().brokers().values()) {
            Long last = stateMachine.lastHeartbeats().putIfAbsent(b.id(), now);
            if (last != null && now - last > TimeUnit.MILLISECONDS.toNanos(sessionTimeoutMs) && fencingInFlight.add(b.id())) {
                LOGGER.warning("Broker " + b.id() + " missed heartbeats for > " + sessionTimeoutMs + " ms; unregistering");
                CompletableFuture.runAsync(() -> {
                    try {
                        submit(MetadataCommand.unregisterBroker(b.id(), b.incarnation()));
                        stateMachine.lastHeartbeats().remove(b.id());
                    } catch (IOException | RuntimeException e) {
                        LOGGER.log(Level.WARNING, "Failed to unregister silent broker " + b.id(), e);
                    } finally {
                        fencingInFlight.remove(b.id());
                    }
                });
            }
        }
    }

    public boolean isLeader() {
        try {
            return server != null && server.getDivision(GROUP_ID).getInfo().isLeader();
        } catch (IOException e) {
            return false;
        }
    }

    /** Current leader's node id, or -1 if unknown (election in progress). */
    public int leaderId() {
        try {
            RaftPeerId leader = server.getDivision(GROUP_ID).getInfo().getLeaderId();
            return leader == null ? -1 : Integer.parseInt(leader.toString().substring(1));
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    public RaftProtos.RaftPeerRole role() throws IOException {
        return server.getDivision(GROUP_ID).getInfo().getCurrentRole();
    }

    private RaftConfiguration conf() throws IOException {
        return server.getDivision(GROUP_ID).getRaftConf();
    }

    public Set<Integer> voterIds() throws IOException {
        return ids(conf().getCurrentPeers(RaftProtos.RaftPeerRole.FOLLOWER));
    }

    public Set<Integer> listenerIds() throws IOException {
        return ids(conf().getCurrentPeers(RaftProtos.RaftPeerRole.LISTENER));
    }

    private static Set<Integer> ids(Collection<RaftPeer> peers) {
        Set<Integer> ids = new TreeSet<>();
        peers.forEach(p -> ids.add(Integer.parseInt(p.getId().toString().substring(1))));
        return ids;
    }

    /** Adds a node as a non-voting listener; it starts replicating immediately. */
    public void addListener(Peer peer) throws IOException {
        List<RaftPeer> voterPeers = new ArrayList<>(conf().getCurrentPeers(RaftProtos.RaftPeerRole.FOLLOWER));
        List<RaftPeer> listenerPeers = new ArrayList<>(conf().getCurrentPeers(RaftProtos.RaftPeerRole.LISTENER));
        listenerPeers.add(peer.toRaft(RaftProtos.RaftPeerRole.LISTENER));
        reconfigure(voterPeers, listenerPeers);
    }

    /**
     * Promotes caught-up listeners to voters.
     *
     * @throws IllegalArgumentException if the result would have an even number of voters
     * @throws IllegalStateException    if a listener is unknown or still catching up
     */
    public void promote(Collection<Integer> listenerIds) throws IOException {
        RaftConfiguration conf = conf();
        List<RaftPeer> voterPeers = new ArrayList<>(conf.getCurrentPeers(RaftProtos.RaftPeerRole.FOLLOWER));
        List<RaftPeer> listenerPeers = new ArrayList<>(conf.getCurrentPeers(RaftProtos.RaftPeerRole.LISTENER));
        int newVoterCount = voterPeers.size() + listenerIds.size();
        if (newVoterCount % 2 == 0) {
            throw new IllegalArgumentException("Refusing promotion to " + newVoterCount
                    + " voters: an even quorum tolerates no more failures than one fewer voter but needs a larger majority");
        }
        long leaderCommit = commitIndexOf(self.raftId());
        for (int id : listenerIds) {
            RaftPeer learner = listenerPeers.stream().filter(p -> p.getId().toString().equals("n" + id)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Node " + id + " is not a listener"));
            long learnerCommit = commitIndexOf(learner.getId());
            if (leaderCommit - learnerCommit > CATCH_UP_SLACK) {
                throw new IllegalStateException("Node " + id + " is still catching up (commit " + learnerCommit
                        + " vs leader " + leaderCommit + ")");
            }
            listenerPeers.remove(learner);
            voterPeers.add(RaftPeer.newBuilder(learner).setStartupRole(RaftProtos.RaftPeerRole.FOLLOWER).build());
        }
        reconfigure(voterPeers, listenerPeers);
    }

    private long commitIndexOf(RaftPeerId id) throws IOException {
        for (RaftProtos.CommitInfoProto info : server.getDivision(GROUP_ID).getCommitInfos()) {
            if (info.getServer().getId().toStringUtf8().equals(id.toString())) {
                return info.getCommitIndex();
            }
        }
        return -1;
    }

    private void reconfigure(List<RaftPeer> voterPeers, List<RaftPeer> listenerPeers) throws IOException {
        RaftClientReply reply = client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
                .setServersInNewConf(voterPeers)
                .setListenersInNewConf(listenerPeers)
                .build());
        if (!reply.isSuccess()) {
            throw new IOException("Reconfiguration failed", reply.getException());
        }
    }

    public void transferLeadership(int toId, long timeoutMs) throws IOException {
        RaftClientReply reply = client.admin().transferLeadership(RaftPeerId.valueOf("n" + toId), timeoutMs);
        if (!reply.isSuccess()) {
            throw new IOException("Leadership transfer to " + toId + " failed", reply.getException());
        }
    }

    @Override
    public void close() throws IOException {
        timers.shutdownNow();
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }
}
