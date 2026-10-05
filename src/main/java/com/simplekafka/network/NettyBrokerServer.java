package com.simplekafka.network;

import com.simplekafka.broker.SimpleKafkaBroker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty 4.2-ready network server for SimpleKafka.
 *
 * ARCHITECTURAL GUARANTEES:
 * 1. Zero Event-Loop Blocking: Disk I/O, group commits, and storage append operations
 *    execute in a separate ExecutorService (requestExecutionPool), keeping the Netty
 *    NioEventLoop threads 100% focused on socket multiplexing.
 * 2. Strict Framing: 4-byte big-endian length headers prevent half-frame corruption.
 * 3. Idempotent Producer Support: Tracks sequence numbers and fences zombie producer instances.
 * 4. Client Quotas: Protects cluster throughput using token-bucket rate limiting.
 */
public class NettyBrokerServer {
    private static final Logger LOGGER = Logger.getLogger(NettyBrokerServer.class.getName());
    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024; // 16 MB max frame

    public static final int DEFAULT_REQUEST_QUEUE_CAPACITY = 1000;

    private final String host;
    private int port;
    private final SimpleKafkaBroker broker;
    private final ClientQuotaManager quotaManager;
    private final IdempotenceManager idempotenceManager;
    private final ProducerIdManager producerIdManager;
    private final int requestQueueCapacity;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ExecutorService requestExecutionPool;
    private Channel serverChannel;

    public NettyBrokerServer(String host, int port, SimpleKafkaBroker broker) {
        this(host, port, broker, new ClientQuotaManager(), new IdempotenceManager(),
                new ProducerIdManager((short) broker.getBrokerId()), DEFAULT_REQUEST_QUEUE_CAPACITY);
    }

    public NettyBrokerServer(
            String host,
            int port,
            SimpleKafkaBroker broker,
            ClientQuotaManager quotaManager,
            IdempotenceManager idempotenceManager,
            ProducerIdManager producerIdManager
    ) {
        this(host, port, broker, quotaManager, idempotenceManager, producerIdManager, DEFAULT_REQUEST_QUEUE_CAPACITY);
    }

    public NettyBrokerServer(
            String host,
            int port,
            SimpleKafkaBroker broker,
            ClientQuotaManager quotaManager,
            IdempotenceManager idempotenceManager,
            ProducerIdManager producerIdManager,
            int requestQueueCapacity
    ) {
        this.host = host;
        this.port = port;
        this.broker = broker;
        this.quotaManager = quotaManager;
        this.idempotenceManager = idempotenceManager;
        this.producerIdManager = producerIdManager;
        this.requestQueueCapacity = requestQueueCapacity;
    }

    public synchronized void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1, new NamedThreadFactory("netty-boss"));
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new NamedThreadFactory("netty-worker"));

        requestExecutionPool = new java.util.concurrent.ThreadPoolExecutor(
                16,
                16,
                0L,
                java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(requestQueueCapacity),
                new NamedThreadFactory("kafka-request-exec"),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy()
        );

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 1. Inbound framing: 4-byte length prefix decoder (stripping the 4 bytes header)
                        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_BYTES, 0, 4, 0, 4));
                        // 2. Outbound framing: 4-byte length prefix prepender
                        pipeline.addLast("framePrepender", new LengthFieldPrepender(4));
                        // 3. KafkaFrame protocol codec
                        pipeline.addLast("kafkaCodec", new KafkaFrameCodec());
                        // 4. Request dispatcher (dispatches work to requestExecutionPool)
                        pipeline.addLast("handler", new NettyServerHandler(
                                broker, requestExecutionPool, quotaManager, idempotenceManager, producerIdManager
                        ));
                    }
                });

        ChannelFuture future = bootstrap.bind(new InetSocketAddress(host, port)).sync();
        this.serverChannel = future.channel();
        this.port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        LOGGER.info("NettyBrokerServer started on " + host + ":" + port);
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().awaitUninterruptibly(2000);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().awaitUninterruptibly(2000);
        }
        if (requestExecutionPool != null) {
            requestExecutionPool.shutdown();
        }
        LOGGER.info("NettyBrokerServer stopped");
    }

    public int getPort() {
        return port;
    }

    public ClientQuotaManager getQuotaManager() {
        return quotaManager;
    }

    public IdempotenceManager getIdempotenceManager() {
        return idempotenceManager;
    }

    public ProducerIdManager getProducerIdManager() {
        return producerIdManager;
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger count = new AtomicInteger(0);

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + count.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
