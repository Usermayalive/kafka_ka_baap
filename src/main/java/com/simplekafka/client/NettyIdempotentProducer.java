package com.simplekafka.client;

import com.simplekafka.network.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance network producer backed by Netty 4.2 pipeline with built-in
 * idempotency, monotonic sequence tracking, epoch handling, and duplicate deduplication.
 */
public class NettyIdempotentProducer implements AutoCloseable {
    private final String clientId;
    private final String host;
    private final int port;

    private EventLoopGroup group;
    private Channel channel;
    private final AtomicInteger correlationIdSeq = new AtomicInteger(0);
    private final Map<Integer, CompletableFuture<KafkaFrame>> inFlightRequests = new ConcurrentHashMap<>();

    private long producerId = -1;
    private short epoch = 0;
    private final Map<String, AtomicInteger> partitionSequenceNumbers = new ConcurrentHashMap<>();

    public record ProduceRecordResult(String topic, int partition, long offset, boolean isDuplicate) {}

    public NettyIdempotentProducer(String clientId, String host, int port) {
        this.clientId = clientId;
        this.host = host;
        this.port = port;
    }

    public synchronized void connect() throws Exception {
        group = new NioEventLoopGroup(2);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(NettyBrokerServer.MAX_FRAME_BYTES, 0, 4, 0, 4));
                        pipeline.addLast("framePrepender", new LengthFieldPrepender(4));
                        pipeline.addLast("codec", new KafkaFrameCodec());
                        pipeline.addLast("clientHandler", new SimpleChannelInboundHandler<KafkaFrame>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, KafkaFrame frame) {
                                CompletableFuture<KafkaFrame> future = inFlightRequests.remove(frame.getCorrelationId());
                                if (future != null) {
                                    future.complete(frame);
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                for (CompletableFuture<KafkaFrame> f : inFlightRequests.values()) {
                                    f.completeExceptionally(cause);
                                }
                                inFlightRequests.clear();
                                ctx.close();
                            }
                        });
                    }
                });

        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
        this.channel = future.channel();

        // Negotiate PID and initial epoch
        initProducerId();
    }

    private void initProducerId() throws Exception {
        int corrId = correlationIdSeq.incrementAndGet();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(clientId);
        dos.flush();

        KafkaFrame request = new KafkaFrame(KafkaFrame.API_KEY_INIT_PRODUCER_ID, (short) 1, corrId, baos.toByteArray());
        CompletableFuture<KafkaFrame> future = new CompletableFuture<>();
        inFlightRequests.put(corrId, future);
        channel.writeAndFlush(request);

        KafkaFrame response = future.get(5, TimeUnit.SECONDS);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(response.getPayload()));
        short errorCode = dis.readShort();
        if (errorCode != KafkaFrame.ERROR_NONE) {
            throw new IOException("Failed to initialize producer ID, broker error: " + errorCode);
        }
        this.producerId = dis.readLong();
        this.epoch = dis.readShort();
    }

    public ProduceRecordResult produce(String topic, int partition, byte[] message) throws Exception {
        String key = topic + "-" + partition;
        int seq = partitionSequenceNumbers.computeIfAbsent(key, k -> new AtomicInteger(0)).getAndIncrement();
        return produceWithSequence(topic, partition, message, seq, epoch);
    }

    public ProduceRecordResult produceWithSequence(String topic, int partition, byte[] message, int sequenceNumber) throws Exception {
        return produceWithSequence(topic, partition, message, sequenceNumber, this.epoch);
    }

    public CompletableFuture<ProduceRecordResult> produceAsync(String topic, int partition, byte[] message, int sequenceNumber) throws IOException {
        return produceAsync(topic, partition, message, sequenceNumber, this.epoch);
    }

    public CompletableFuture<ProduceRecordResult> produceAsync(String topic, int partition, byte[] message, int sequenceNumber, short explicitEpoch) throws IOException {
        int corrId = correlationIdSeq.incrementAndGet();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(clientId);
        dos.writeUTF(topic);
        dos.writeInt(partition);
        dos.writeLong(producerId);
        dos.writeShort(explicitEpoch);
        dos.writeInt(sequenceNumber);
        dos.writeInt(message.length);
        dos.write(message);
        dos.flush();

        KafkaFrame request = new KafkaFrame(KafkaFrame.API_KEY_PRODUCE, (short) 1, corrId, baos.toByteArray());
        CompletableFuture<KafkaFrame> frameFuture = new CompletableFuture<>();
        inFlightRequests.put(corrId, frameFuture);
        channel.writeAndFlush(request);

        return frameFuture.thenApply(response -> {
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(response.getPayload()));
                short errorCode = dis.readShort();

                if (errorCode == KafkaFrame.ERROR_NONE) {
                    String respTopic = dis.readUTF();
                    int respPartition = dis.readInt();
                    long assignedOffset = dis.readLong();
                    boolean isDuplicate = dis.readBoolean();
                    return new ProduceRecordResult(respTopic, respPartition, assignedOffset, isDuplicate);
                } else if (errorCode == KafkaFrame.ERROR_PRODUCER_FENCED) {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new ProducerFencedException("Produce rejected - producer fenced: " + errorMsg));
                } else if (errorCode == KafkaFrame.ERROR_OUT_OF_ORDER_SEQUENCE) {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new OutOfOrderSequenceException("Produce rejected - out of order sequence: " + errorMsg));
                } else if (errorCode == KafkaFrame.ERROR_QUOTA_EXCEEDED) {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new QuotaExceededException(clientId, 100, "Produce rejected - quota exceeded: " + errorMsg));
                } else if (errorCode == KafkaFrame.ERROR_BROKER_OVERLOADED) {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new BrokerOverloadedException("Produce rejected - broker overloaded: " + errorMsg));
                } else if (errorCode == KafkaFrame.ERROR_DUPLICATE_SEQUENCE) {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new DuplicateSequenceException("Produce rejected - duplicate sequence out of window: " + errorMsg));
                } else {
                    String errorMsg = dis.readUTF();
                    throw new CompletionException(new IOException("Produce failed with error " + errorCode + ": " + errorMsg));
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    public ProduceRecordResult produceWithSequence(String topic, int partition, byte[] message, int sequenceNumber, short explicitEpoch) throws Exception {
        try {
            return produceAsync(topic, partition, message, sequenceNumber, explicitEpoch).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof Exception exc) {
                throw exc;
            }
            throw ee;
        }
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public void setEpoch(short epoch) {
        this.epoch = epoch;
    }

    @Override
    public synchronized void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (group != null) {
            group.shutdownGracefully().awaitUninterruptibly(1000);
        }
    }
}
