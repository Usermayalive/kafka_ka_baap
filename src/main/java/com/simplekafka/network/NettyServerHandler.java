package com.simplekafka.network;

import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty inbound handler implementing request dispatch, event-loop offloading,
 * quota enforcement, and idempotent deduplication.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<KafkaFrame> {
    private static final Logger LOGGER = Logger.getLogger(NettyServerHandler.class.getName());

    /** Per-connection queued request count at which the socket stops being read. */
    public static final int PENDING_REQUESTS_HIGH_WATERMARK = 256;
    /** Per-connection queued request count at which reading resumes. */
    public static final int PENDING_REQUESTS_LOW_WATERMARK = 64;

    private final SimpleKafkaBroker broker;
    private final ExecutorService requestExecutionPool;
    private final ClientQuotaManager quotaManager;
    private final IdempotenceManager idempotenceManager;
    private final ProducerIdManager producerIdManager;
    private final SerializingExecutor channelExecutor;

    public NettyServerHandler(
            SimpleKafkaBroker broker,
            ExecutorService requestExecutionPool,
            ClientQuotaManager quotaManager,
            IdempotenceManager idempotenceManager,
            ProducerIdManager producerIdManager
    ) {
        this.broker = broker;
        this.requestExecutionPool = requestExecutionPool;
        this.quotaManager = quotaManager;
        this.idempotenceManager = idempotenceManager;
        this.producerIdManager = producerIdManager;
        this.channelExecutor = new SerializingExecutor(requestExecutionPool);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KafkaFrame frame) {
        // NON-BLOCKING EVENT-LOOP + CHANNEL ORDERING GUARANTEE:
        // SerializingExecutor executes tasks sequentially per channel on the worker pool,
        // preventing out-of-order execution of pipelined requests while offloading disk I/O.
        try {
            channelExecutor.execute(() -> {
                long started = System.nanoTime();
                try {
                    KafkaFrame response = processFrame(frame);
                    broker.getMetrics().recordRequest(apiName(frame.getApiKey()), System.nanoTime() - started,
                            response.getPayload().length >= 2 && (response.getPayload()[0] != 0 || response.getPayload()[1] != 0));
                    ctx.writeAndFlush(response);
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Unexpected error processing frame: " + frame, t);
                    KafkaFrame errorResponse = buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                            KafkaFrame.ERROR_SERVER_ERROR, t.getMessage());
                    ctx.writeAndFlush(errorResponse);
                } finally {
                    maybeResumeReading(ctx);
                }
            });
            pauseReadingIfBacklogged(ctx);
        } catch (java.util.concurrent.RejectedExecutionException ree) {
            LOGGER.warning("Broker overloaded: request execution queue full, rejecting frame " + frame.getCorrelationId());
            KafkaFrame overloadResponse = buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_BROKER_OVERLOADED, "BROKER_OVERLOADED: Request execution queue full");
            ctx.writeAndFlush(overloadResponse);
        }
    }

    /**
     * Per-connection backpressure. Runs on the event loop right after enqueueing, so the depth
     * is current. Stopping reads lets the socket buffers fill and TCP flow control throttle the
     * client. Frames already decoded from the current read still arrive, so the backlog can
     * overshoot the high watermark by up to one read's worth.
     */
    private void pauseReadingIfBacklogged(ChannelHandlerContext ctx) {
        if (channelExecutor.getQueueDepth() >= PENDING_REQUESTS_HIGH_WATERMARK && ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(false);
            LOGGER.fine("Connection backlog at " + channelExecutor.getQueueDepth() +
                    " requests. Paused autoRead on channel: " + ctx.channel().id());
        }
    }

    /**
     * Called after every request completes. Any task finishing after a pause observes
     * autoRead == false, and a pause implies queued tasks remain, so reads always resume.
     */
    private void maybeResumeReading(ChannelHandlerContext ctx) {
        if (!ctx.channel().config().isAutoRead() && channelExecutor.getQueueDepth() <= PENDING_REQUESTS_LOW_WATERMARK) {
            ctx.channel().config().setAutoRead(true);
            LOGGER.fine("Connection backlog drained to " + channelExecutor.getQueueDepth() +
                    " requests. Resumed autoRead on channel: " + ctx.channel().id());
        }
    }

    private static String apiName(short apiKey) {
        return switch (apiKey) {
            case KafkaFrame.API_KEY_PRODUCE -> "produce";
            case KafkaFrame.API_KEY_FETCH -> "fetch";
            case KafkaFrame.API_KEY_INIT_PRODUCER_ID -> "init_producer_id";
            case KafkaFrame.API_KEY_METADATA -> "metadata";
            case KafkaFrame.API_KEY_CREATE_TOPIC -> "create_topic";
            case KafkaFrame.API_KEY_FIND_COORDINATOR -> "find_coordinator";
            case KafkaFrame.API_KEY_OFFSET_COMMIT -> "offset_commit";
            case KafkaFrame.API_KEY_OFFSET_FETCH -> "offset_fetch";
            case KafkaFrame.API_KEY_JOIN_GROUP -> "join_group";
            case KafkaFrame.API_KEY_SYNC_GROUP -> "sync_group";
            case KafkaFrame.API_KEY_HEARTBEAT -> "heartbeat";
            case KafkaFrame.API_KEY_LEAVE_GROUP -> "leave_group";
            case KafkaFrame.API_KEY_REGISTER_SCHEMA -> "register_schema";
            case KafkaFrame.API_KEY_SET_SCHEMA_COMPATIBILITY -> "set_schema_compatibility";
            case KafkaFrame.API_KEY_GET_SCHEMA -> "get_schema";
            case KafkaFrame.API_KEY_FORGET_ENTITY -> "forget_entity";
            case KafkaFrame.API_KEY_REPLICA_FETCH -> "replica_fetch";
            default -> "unknown"; // bounded: never the raw key
        };
    }

    private KafkaFrame processFrame(KafkaFrame frame) throws Exception {
        switch (frame.getApiKey()) {
            case KafkaFrame.API_KEY_INIT_PRODUCER_ID:
                return handleInitProducerId(frame);
            case KafkaFrame.API_KEY_PRODUCE:
                return handleProduce(frame);
            case KafkaFrame.API_KEY_FETCH:
                return handleFetch(frame);
            case KafkaFrame.API_KEY_METADATA:
                return handleMetadata(frame);
            case KafkaFrame.API_KEY_CREATE_TOPIC:
                return handleCreateTopic(frame);
            case KafkaFrame.API_KEY_FIND_COORDINATOR:
                return handleFindCoordinator(frame);
            case KafkaFrame.API_KEY_OFFSET_COMMIT:
                return handleOffsetCommit(frame);
            case KafkaFrame.API_KEY_OFFSET_FETCH:
                return handleOffsetFetch(frame);
            case KafkaFrame.API_KEY_JOIN_GROUP:
            case KafkaFrame.API_KEY_SYNC_GROUP:
            case KafkaFrame.API_KEY_HEARTBEAT:
            case KafkaFrame.API_KEY_LEAVE_GROUP:
                return handleGroupRequest(frame);
            case KafkaFrame.API_KEY_REGISTER_SCHEMA:
                return handleRegisterSchema(frame);
            case KafkaFrame.API_KEY_SET_SCHEMA_COMPATIBILITY:
                return handleSetCompatibility(frame);
            case KafkaFrame.API_KEY_GET_SCHEMA:
                return handleGetSchema(frame);
            case KafkaFrame.API_KEY_FORGET_ENTITY:
                return handleForgetEntity(frame);
            case KafkaFrame.API_KEY_REPLICA_FETCH:
                if (broker.getReplicaManager() == null) {
                    return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_NOT_LEADER,
                            "Replication is not running on this broker");
                }
                return broker.getReplicaManager().handleReplicaFetch(frame);
            default:
                return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                        KafkaFrame.ERROR_CORRUPT_MESSAGE, "Unknown apiKey: " + frame.getApiKey());
        }
    }

    private KafkaFrame handleInitProducerId(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        String clientId = dis.readUTF();

        ProducerIdManager.ProducerIdAndEpoch idAndEpoch = producerIdManager.generateProducerId();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        dos.writeLong(idAndEpoch.producerId());
        dos.writeShort(idAndEpoch.epoch());
        dos.flush();

        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
    }

    private KafkaFrame handleProduce(KafkaFrame frame) throws Exception {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        String clientId = dis.readUTF();
        String topic = dis.readUTF();
        int partitionId = dis.readInt();
        long producerId = dis.readLong();
        short epoch = dis.readShort();
        int sequenceNumber = dis.readInt();
        int msgLen = dis.readInt();
        byte[] message = new byte[msgLen];
        dis.readFully(message);
        // Optional trailing acks byte; older clients omit it and get leader-only acks
        byte acks = dis.available() > 0 ? dis.readByte() : KafkaFrame.ACKS_LEADER;

        // 1. Enforce Client Rate Quotas
        try {
            quotaManager.checkAndConsume(clientId, message.length);
        } catch (QuotaExceededException qe) {
            LOGGER.warning("Quota exceeded for client: " + clientId + " - " + qe.getMessage());
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_QUOTA_EXCEEDED, qe.getMessage());
        }

        // 2. Resolve Partition
        if (broker.hostedPartition(topic, partitionId) == null && topic.endsWith(".dlq")) {
            broker.createTopic(topic, 1, (short) 1); // companion DLQ auto-creation
        }
        Partition partition = broker.hostedPartition(topic, partitionId);
        KafkaFrame notLeader = rejectIfNotLeader(frame, topic, partitionId, partition);
        if (notLeader != null) {
            return notLeader;
        }
        // 3. Schema enforcement and PII encryption (topics with a registered schema only)
        final byte[] stored;
        try {
            stored = broker.getSchemaService().onProduce(topic, message);
        } catch (com.simplekafka.schema.SchemaViolationException sve) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_SCHEMA_VIOLATION,
                    sve.getMessage());
        }
        broker.getMetrics().recordProduce(topic, stored.length);
        if (acks == KafkaFrame.ACKS_ALL) {
            java.util.Set<Integer> isr = broker.getReplicaManager() == null ? null
                    : broker.getReplicaManager().isrOf(topic, partitionId);
            if (isr != null && isr.size() < broker.getMinInsyncReplicas()) {
                return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_NOT_ENOUGH_REPLICAS,
                        topic + "-" + partitionId + " has " + isr.size() + " in-sync replica(s) " + isr
                                + "; acks=all needs at least " + broker.getMinInsyncReplicas());
            }
        }
        if (producerId == KafkaFrame.NO_PRODUCER_ID) {
            long offset = partition.append(stored);
            KafkaFrame notReplicated = awaitReplication(frame, partition, offset, acks);
            return notReplicated != null ? notReplicated : produceResponse(frame, topic, partitionId, offset, false);
        }
        idempotenceManager.registerPartitionDir(topic, partitionId, new File(partition.getBaseDir()));

        // 3. Idempotent Sequence & Epoch Verification
        try {
            // Only the page-cache write happens under the idempotence lock; the fsync wait (if any)
            // happens after it is released so concurrent producers share one group commit. A
            // duplicate also waits: its original may still be awaiting the same flush.
            IdempotenceManager.IdempotentResult result = idempotenceManager.verifyAndApply(
                    topic, partitionId, producerId, epoch, sequenceNumber, () -> partition.appendBuffered(stored)
            );
            partition.awaitDurable(result.offset());
            KafkaFrame notReplicated = awaitReplication(frame, partition, result.offset(), acks);
            if (notReplicated != null) {
                return notReplicated;
            }
            return produceResponse(frame, topic, partitionId, result.offset(), result.isDuplicate());
        } catch (ProducerFencedException pfe) {
            LOGGER.warning("Producer fenced: " + pfe.getMessage());
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_PRODUCER_FENCED, pfe.getMessage());
        } catch (DuplicateSequenceException dse) {
            LOGGER.warning("Duplicate sequence out of window: " + dse.getMessage());
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_DUPLICATE_SEQUENCE, dse.getMessage());
        } catch (OutOfOrderSequenceException oose) {
            LOGGER.warning("Out of order sequence: " + oose.getMessage());
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_OUT_OF_ORDER_SEQUENCE, oose.getMessage());
        }
    }

    private KafkaFrame handleFetch(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        String clientId = dis.readUTF();
        String topic = dis.readUTF();
        int partitionId = dis.readInt();
        long fetchOffset = dis.readLong();
        int maxBytes = dis.readInt();

        Partition partition = broker.hostedPartition(topic, partitionId);
        KafkaFrame notLeader = rejectIfNotLeader(frame, topic, partitionId, partition);
        if (notLeader != null) {
            return notLeader;
        }
        // Consumers only see committed records: below the high watermark (= log end if unreplicated)
        long highWatermark = partition.getHighWatermark();
        List<Partition.Record> records = partition.readRecords(fetchOffset, maxBytes, highWatermark);

        // [error][count]{[offset:8][len:4][payload]}[logEndOffset:8]
        // Offsets are explicit: compacted partitions have gaps.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        dos.writeInt(records.size());
        for (Partition.Record r : records) {
            byte[] payload = broker.getSchemaService().onFetch(topic, r.payload());
            dos.writeLong(r.offset());
            dos.writeInt(payload.length);
            dos.write(payload);
        }
        dos.writeLong(highWatermark);
        dos.flush();

        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
    }

    private KafkaFrame produceResponse(KafkaFrame frame, String topic, int partitionId, long offset, boolean duplicate)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        dos.writeUTF(topic);
        dos.writeInt(partitionId);
        dos.writeLong(offset);
        dos.writeBoolean(duplicate);
        dos.flush();
        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
    }

    /**
     * Request: [clientId]. Response: [error][brokerCount]{[id:4][host][port:4]}
     * [topicCount]{[name][partitionCount]{[partition:4][leader:4][replicaCount:4]{[replica:4]}}}
     */
    private KafkaFrame handleMetadata(KafkaFrame frame) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        List<com.simplekafka.broker.BrokerInfo> brokers = broker.liveBrokers();
        dos.writeInt(brokers.size());
        for (com.simplekafka.broker.BrokerInfo b : brokers) {
            dos.writeInt(b.getId());
            dos.writeUTF(b.getHost());
            dos.writeInt(b.getPort());
        }
        List<SimpleKafkaBroker.TopicDescription> topics = broker.describeTopics();
        dos.writeInt(topics.size());
        for (SimpleKafkaBroker.TopicDescription t : topics) {
            dos.writeUTF(t.name());
            dos.writeInt(t.partitions().size());
            for (SimpleKafkaBroker.PartitionDescription p : t.partitions()) {
                dos.writeInt(p.partitionId());
                dos.writeInt(p.leader());
                dos.writeInt(p.replicas().size());
                for (int replica : p.replicas()) {
                    dos.writeInt(replica);
                }
            }
        }
        dos.flush();
        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
    }

    /** Request: [clientId][topic][partitions:4][replicationFactor:2]. Only the controller creates topics. */
    private KafkaFrame handleCreateTopic(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String topic = dis.readUTF();
        int partitions = dis.readInt();
        short replicationFactor = dis.readShort();
        if (!broker.canCreateTopics()) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_NOT_CONTROLLER, "Broker " + broker.getBrokerId() + " is not the controller");
        }
        broker.createTopic(topic, partitions, replicationFactor);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new DataOutputStream(baos).writeShort(KafkaFrame.ERROR_NONE);
        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
    }

    /** Request: [clientId][group]. Response: [error][brokerId:4][host][port:4]. */
    private KafkaFrame handleFindCoordinator(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String group = dis.readUTF();
        com.simplekafka.broker.BrokerInfo coordinator = broker.coordinatorFor(group);
        if (coordinator == null) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_COORDINATOR_NOT_AVAILABLE, "No coordinator available for group " + group);
        }
        return okFrame(frame, out -> {
            out.writeInt(coordinator.getId());
            out.writeUTF(coordinator.getHost());
            out.writeInt(coordinator.getPort());
        });
    }

    /** Request: [clientId][group][topic][partition:4][offset:8][metadata]. Response: [error]. */
    private KafkaFrame handleOffsetCommit(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String group = dis.readUTF();
        String topic = dis.readUTF();
        int partition = dis.readInt();
        long offset = dis.readLong();
        String metadata = dis.readUTF();
        KafkaFrame notCoordinator = rejectIfNotCoordinator(frame, group);
        if (notCoordinator != null) {
            return notCoordinator;
        }
        broker.getOffsetStore().commit(group, topic, partition, offset, metadata);
        return okFrame(frame, out -> {});
    }

    /** Request: [clientId][group][topic][partition:4]. Response: [error][offset:8] (-1 if none)[metadata]. */
    private KafkaFrame handleOffsetFetch(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String group = dis.readUTF();
        String topic = dis.readUTF();
        int partition = dis.readInt();
        KafkaFrame notCoordinator = rejectIfNotCoordinator(frame, group);
        if (notCoordinator != null) {
            return notCoordinator;
        }
        var committed = broker.getOffsetStore().fetch(group, topic, partition);
        return okFrame(frame, out -> {
            out.writeLong(committed == null ? -1L : committed.offset());
            out.writeUTF(committed == null ? "" : committed.metadata());
        });
    }

    /**
     * JOIN:      [clientId][group][memberId][instanceId][sessionTimeoutMs:4][n]{topic}
     *            -> [error][memberId][generation:4]
     * SYNC:      [clientId][group][memberId][n]{[topic][partition:4]} (owned)
     *            -> [error][generation:4][n]{[topic][partition:4]} (grantable assignment)
     * HEARTBEAT: same request as SYNC -> [error][generation:4][needsSync:1]
     * LEAVE:     [clientId][group][memberId] -> [error]
     */
    private KafkaFrame handleGroupRequest(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String group = dis.readUTF();
        KafkaFrame notCoordinator = rejectIfNotCoordinator(frame, group);
        if (notCoordinator != null) {
            return notCoordinator;
        }
        com.simplekafka.coordinator.GroupCoordinator coordinator = broker.getGroupCoordinator();
        String memberId = dis.readUTF();
        try {
            switch (frame.getApiKey()) {
                case KafkaFrame.API_KEY_JOIN_GROUP: {
                    String instanceId = dis.readUTF();
                    long sessionTimeoutMs = dis.readInt();
                    java.util.Set<String> topics = new java.util.TreeSet<>();
                    for (int n = dis.readInt(); n > 0; n--) {
                        topics.add(dis.readUTF());
                    }
                    var joined = coordinator.join(group, memberId, instanceId, topics, sessionTimeoutMs);
                    return okFrame(frame, out -> {
                        out.writeUTF(joined.memberId());
                        out.writeInt(joined.generation());
                    });
                }
                case KafkaFrame.API_KEY_SYNC_GROUP: {
                    var synced = coordinator.sync(group, memberId, readPartitions(dis));
                    return okFrame(frame, out -> {
                        out.writeInt(synced.generation());
                        out.writeInt(synced.assignment().size());
                        for (var tp : synced.assignment()) {
                            out.writeUTF(tp.topic());
                            out.writeInt(tp.partition());
                        }
                    });
                }
                case KafkaFrame.API_KEY_HEARTBEAT: {
                    var beat = coordinator.heartbeat(group, memberId, readPartitions(dis));
                    return okFrame(frame, out -> {
                        out.writeInt(beat.generation());
                        out.writeBoolean(beat.needsSync());
                    });
                }
                default:
                    coordinator.leave(group, memberId);
                    return okFrame(frame, out -> {});
            }
        } catch (com.simplekafka.coordinator.GroupCoordinator.UnknownMemberException e) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_UNKNOWN_MEMBER, e.getMessage());
        }
    }

    private static java.util.Set<com.simplekafka.coordinator.GroupCoordinator.TopicPartition> readPartitions(DataInputStream dis)
            throws IOException {
        java.util.Set<com.simplekafka.coordinator.GroupCoordinator.TopicPartition> result = new java.util.TreeSet<>();
        for (int n = dis.readInt(); n > 0; n--) {
            result.add(new com.simplekafka.coordinator.GroupCoordinator.TopicPartition(dis.readUTF(), dis.readInt()));
        }
        return result;
    }

    /**
     * Produce and fetch are served only by the partition's leader. Clients refresh metadata and
     * retry on NOT_LEADER / LEADER_NOT_AVAILABLE. A leader of 0 means "unassigned" (standalone
     * brokers without cluster metadata) and is served locally.
     */
    /** Request: [clientId][topic][schemaJson]. Response: [error][schemaId:4][version:4]. */
    private KafkaFrame handleRegisterSchema(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String topic = dis.readUTF();
        String schemaJson = readLongString(dis);
        org.apache.avro.Schema parsed;
        try {
            parsed = new org.apache.avro.Schema.Parser().parse(schemaJson);
            broker.applyMetadata(com.simplekafka.metadata.MetadataCommand.registerSchema(topic, schemaJson));
        } catch (IllegalStateException | org.apache.avro.SchemaParseException e) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_INCOMPATIBLE_SCHEMA,
                    e.getMessage());
        }
        for (var v : broker.metadataImage().schemas(topic)) {
            if (new org.apache.avro.Schema.Parser().parse(v.schema()).equals(parsed)) {
                return okFrame(frame, out -> {
                    out.writeInt(v.id());
                    out.writeInt(v.version());
                });
            }
        }
        return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_SERVER_ERROR,
                "Schema committed but not yet visible on this broker; retry");
    }

    /** Request: [clientId][topic][NONE|BACKWARD|FORWARD|FULL]. Response: [error]. */
    private KafkaFrame handleSetCompatibility(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String topic = dis.readUTF();
        String mode = dis.readUTF();
        try {
            broker.applyMetadata(com.simplekafka.metadata.MetadataCommand.setSchemaCompatibility(topic, mode));
        } catch (IllegalStateException e) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_INCOMPATIBLE_SCHEMA,
                    e.getMessage());
        }
        return okFrame(frame, out -> {});
    }

    /** Request: [clientId][schemaId:4]. Response: [error][schemaJson]. */
    private KafkaFrame handleGetSchema(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        int id = dis.readInt();
        var schema = broker.metadataImage().schemaById(id);
        if (schema == null) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_SCHEMA_NOT_FOUND,
                    "No schema with id " + id);
        }
        return okFrame(frame, out -> writeLongString(out, schema.schema()));
    }

    /** Request: [clientId][entityId]. Response: [error]. Replicated: every broker destroys its key. */
    private KafkaFrame handleForgetEntity(KafkaFrame frame) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame.getPayload()));
        dis.readUTF(); // clientId
        String entityId = dis.readUTF();
        // Only the hash is replicated and persisted; the raw identifier stops here
        broker.applyMetadata(com.simplekafka.metadata.MetadataCommand.forgetEntity(
                com.simplekafka.gdpr.CryptoShredder.entityKey(entityId)));
        return okFrame(frame, out -> {});
    }

    /** Schemas can exceed writeUTF's 64KB limit: [len:4][utf8 bytes]. */
    private static String readLongString(DataInputStream in) throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeLongString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private KafkaFrame rejectIfNotLeader(KafkaFrame frame, String topic, int partitionId, Partition partition) {
        if (!broker.isServingReady()) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_LEADER_NOT_AVAILABLE,
                    "Broker " + broker.getBrokerId() + " is still catching up on cluster metadata");
        }
        if (partition == null) {
            boolean known = broker.partitionCount(topic) > partitionId;
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    known ? KafkaFrame.ERROR_NOT_LEADER : KafkaFrame.ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                    (known ? "Not the leader for " : "Topic or partition not found: ") + topic + "-" + partitionId);
        }
        if (partition.getLeader() < 0) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_LEADER_NOT_AVAILABLE,
                    topic + "-" + partitionId + " is offline: its only in-sync replica is down");
        }
        if (partition.getLeader() > 0 && partition.getLeader() != broker.getBrokerId()) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_NOT_LEADER,
                    "Broker " + broker.getBrokerId() + " is not the leader for " + topic + "-" + partitionId
                            + " (leader: " + partition.getLeader() + ")");
        }
        return null;
    }

    /** acks=all: wait until every in-sync replica has the record (the HW passes it). */
    private KafkaFrame awaitReplication(KafkaFrame frame, Partition partition, long offset, byte acks) {
        if (acks != KafkaFrame.ACKS_ALL) {
            return null;
        }
        try {
            if (partition.awaitHighWatermark(offset, 30_000)) {
                return null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Like Kafka: the record is in the leader's log and may still commit; the client retries
        return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_REQUEST_TIMED_OUT,
                "Offset " + offset + " not replicated to all in-sync replicas within 30 s");
    }

    private KafkaFrame rejectIfNotCoordinator(KafkaFrame frame, String group) {
        int offsetsPartition = com.simplekafka.coordinator.ConsumerOffsetStore.partitionFor(group);
        Partition offsetsLog = broker.hostedPartition(com.simplekafka.coordinator.ConsumerOffsetStore.TOPIC, offsetsPartition);
        // A follower replica of the offsets partition is not the coordinator: only its leader is
        if (offsetsLog == null || (offsetsLog.getLeader() > 0 && offsetsLog.getLeader() != broker.getBrokerId())) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(), KafkaFrame.ERROR_NOT_COORDINATOR,
                    "Broker " + broker.getBrokerId() + " is not the coordinator for group " + group);
        }
        return null;
    }

    @FunctionalInterface
    interface BodyWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private KafkaFrame okFrame(KafkaFrame request, BodyWriter body) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        body.write(dos);
        dos.flush();
        return new KafkaFrame(request.getApiKey(), request.getApiVersion(), request.getCorrelationId(), baos.toByteArray());
    }

    private KafkaFrame buildErrorFrame(short apiKey, int correlationId, short errorCode, String errorMsg) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeShort(errorCode);
            dos.writeUTF(errorMsg != null ? errorMsg : "Error " + errorCode);
            dos.flush();
            return new KafkaFrame(apiKey, (short) 1, correlationId, baos.toByteArray());
        } catch (IOException e) {
            return new KafkaFrame(apiKey, (short) 1, correlationId, new byte[]{0, (byte) errorCode});
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof io.netty.handler.codec.TooLongFrameException) {
            LOGGER.severe("Frame size exceeded MAX_FRAME_BYTES (" + NettyBrokerServer.MAX_FRAME_BYTES + "). Closing channel: " + cause.getMessage());
        } else {
            LOGGER.log(Level.WARNING, "Netty channel exception caught", cause);
        }
        ctx.close();
    }
}
