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
        // High-watermark check for autoRead backpressure:
        // When requestExecutionPool queue exceeds 80% capacity, stop reading from socket to throttle upstream
        if (requestExecutionPool instanceof java.util.concurrent.ThreadPoolExecutor tpe) {
            int queueSize = tpe.getQueue().size();
            int remaining = tpe.getQueue().remainingCapacity();
            int total = queueSize + remaining;
            if (total > 0 && queueSize >= (int) (total * 0.8)) {
                ctx.channel().config().setAutoRead(false);
                LOGGER.warning("requestExecutionPool queue at 80%+ capacity (" + queueSize + "/" + total +
                        "). Paused autoRead on channel: " + ctx.channel().id());
            }
        }

        // NON-BLOCKING EVENT-LOOP + CHANNEL ORDERING GUARANTEE:
        // SerializingExecutor executes tasks sequentially per channel on the worker pool,
        // preventing out-of-order execution of pipelined requests while offloading disk I/O.
        try {
            channelExecutor.execute(() -> {
                try {
                    KafkaFrame response = processFrame(frame);
                    ctx.writeAndFlush(response);
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Unexpected error processing frame: " + frame, t);
                    KafkaFrame errorResponse = buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                            KafkaFrame.ERROR_SERVER_ERROR, t.getMessage());
                    ctx.writeAndFlush(errorResponse);
                } finally {
                    // Low-watermark check to resume autoRead when queue drops below 20%
                    if (requestExecutionPool instanceof java.util.concurrent.ThreadPoolExecutor tpe) {
                        int queueSize = tpe.getQueue().size();
                        int remaining = tpe.getQueue().remainingCapacity();
                        int total = queueSize + remaining;
                        if (total > 0 && queueSize <= (int) (total * 0.2)) {
                            if (!ctx.channel().config().isAutoRead()) {
                                ctx.channel().config().setAutoRead(true);
                                LOGGER.info("requestExecutionPool queue drained below 20% (" + queueSize + "/" + total +
                                        "). Resumed autoRead on channel: " + ctx.channel().id());
                            }
                        }
                    }
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ree) {
            LOGGER.warning("Broker overloaded: request execution queue full, rejecting frame " + frame.getCorrelationId());
            KafkaFrame overloadResponse = buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_BROKER_OVERLOADED, "BROKER_OVERLOADED: Request execution queue full");
            ctx.writeAndFlush(overloadResponse);
        }
    }

    private KafkaFrame processFrame(KafkaFrame frame) throws Exception {
        switch (frame.getApiKey()) {
            case KafkaFrame.API_KEY_INIT_PRODUCER_ID:
                return handleInitProducerId(frame);
            case KafkaFrame.API_KEY_PRODUCE:
                return handleProduce(frame);
            case KafkaFrame.API_KEY_FETCH:
                return handleFetch(frame);
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

        // 1. Enforce Client Rate Quotas
        try {
            quotaManager.checkAndConsume(clientId, message.length);
        } catch (QuotaExceededException qe) {
            LOGGER.warning("Quota exceeded for client: " + clientId + " - " + qe.getMessage());
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_QUOTA_EXCEEDED, qe.getMessage());
        }

        // 2. Resolve Partition
        List<Partition> parts = broker.getTopics().get(topic);
        if (parts == null || partitionId >= parts.size()) {
            // Check companion DLQ auto-creation
            if (topic.endsWith(".dlq")) {
                broker.createTopic(topic, 1, (short) 1);
                parts = broker.getTopics().get(topic);
            }
            if (parts == null || partitionId >= parts.size()) {
                return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                        KafkaFrame.ERROR_UNKNOWN_TOPIC_OR_PARTITION, "Topic or partition not found: " + topic + "-" + partitionId);
            }
        }

        Partition partition = parts.get(partitionId);
        idempotenceManager.registerPartitionDir(topic, partitionId, new File(partition.getBaseDir()));

        // 3. Idempotent Sequence & Epoch Verification
        try {
            IdempotenceManager.IdempotentResult result = idempotenceManager.verifyAndApply(
                    topic, partitionId, producerId, epoch, sequenceNumber, () -> partition.append(message)
            );

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeShort(KafkaFrame.ERROR_NONE);
            dos.writeUTF(topic);
            dos.writeInt(partitionId);
            dos.writeLong(result.offset());
            dos.writeBoolean(result.isDuplicate());
            dos.flush();

            return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
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

        List<Partition> parts = broker.getTopics().get(topic);
        if (parts == null || partitionId >= parts.size()) {
            return buildErrorFrame(frame.getApiKey(), frame.getCorrelationId(),
                    KafkaFrame.ERROR_UNKNOWN_TOPIC_OR_PARTITION, "Topic/partition not found: " + topic);
        }

        Partition partition = parts.get(partitionId);
        List<byte[]> messages = partition.readMessages(fetchOffset, maxBytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(KafkaFrame.ERROR_NONE);
        dos.writeInt(messages.size());
        for (byte[] m : messages) {
            dos.writeInt(m.length);
            dos.write(m);
        }
        dos.flush();

        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), baos.toByteArray());
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
