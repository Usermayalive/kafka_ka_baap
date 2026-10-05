package com.simplekafka.client;

import com.simplekafka.network.KafkaFrame;
import com.simplekafka.network.KafkaFrameCodec;
import com.simplekafka.network.NettyBrokerServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pipelined request/response connection to one broker over the length-prefixed KafkaFrame
 * protocol. Responses are matched to requests by correlation ID, so many requests can be in
 * flight at once; the broker answers each connection's requests in order.
 */
public class BrokerConnection implements AutoCloseable {
    private static final long DEFAULT_TIMEOUT_MS = 10_000;

    // Shared across connections: one event loop group per client JVM, daemon threads.
    private static final EventLoopGroup SHARED_GROUP = new NioEventLoopGroup(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "stormlog-client-io");
                t.setDaemon(true);
                return t;
            });

    private final String host;
    private final int port;
    private final Channel channel;
    private final AtomicInteger correlationIds = new AtomicInteger();
    private final Map<Integer, CompletableFuture<KafkaFrame>> inFlight = new ConcurrentHashMap<>();

    private BrokerConnection(String host, int port, Channel channel) {
        this.host = host;
        this.port = port;
        this.channel = channel;
    }

    public static BrokerConnection connect(String host, int port) throws IOException {
        BrokerConnection[] holder = new BrokerConnection[1];
        Bootstrap bootstrap = new Bootstrap()
                .group(SHARED_GROUP)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(NettyBrokerServer.MAX_FRAME_BYTES, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new KafkaFrameCodec())
                                .addLast(new SimpleChannelInboundHandler<KafkaFrame>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, KafkaFrame frame) {
                                        CompletableFuture<KafkaFrame> f = holder[0].inFlight.remove(frame.getCorrelationId());
                                        if (f != null) {
                                            f.complete(frame);
                                        }
                                    }

                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) {
                                        holder[0].failAll(new IOException("Connection to " + host + ":" + port + " closed"));
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        holder[0].failAll(cause);
                                        ctx.close();
                                    }
                                });
                    }
                });
        try {
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            holder[0] = new BrokerConnection(host, port, future.channel());
            return holder[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted connecting to " + host + ":" + port, e);
        } catch (Exception e) {
            throw new IOException("Failed to connect to " + host + ":" + port, e);
        }
    }

    public CompletableFuture<KafkaFrame> sendAsync(short apiKey, byte[] payload) {
        int correlationId = correlationIds.incrementAndGet();
        CompletableFuture<KafkaFrame> future = new CompletableFuture<>();
        inFlight.put(correlationId, future);
        channel.writeAndFlush(new KafkaFrame(apiKey, (short) 1, correlationId, payload))
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        inFlight.remove(correlationId);
                        future.completeExceptionally(f.cause());
                    }
                });
        return future;
    }

    /** Sends a request and returns the response body positioned after a successful error code. */
    public DataInputStream request(short apiKey, byte[] payload) throws IOException {
        return body(await(sendAsync(apiKey, payload)));
    }

    public static KafkaFrame await(CompletableFuture<KafkaFrame> future) throws IOException {
        try {
            return future.get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for broker response", e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("Timed out waiting for broker response", e);
        }
    }

    /** Reads the error code; throws BrokerErrorException for anything but ERROR_NONE. */
    public static DataInputStream body(KafkaFrame response) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(response.getPayload()));
        short errorCode = in.readShort();
        if (errorCode != KafkaFrame.ERROR_NONE) {
            String message = in.available() > 0 ? in.readUTF() : "error " + errorCode;
            throw new BrokerErrorException(errorCode, message);
        }
        return in;
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public String address() {
        return host + ":" + port;
    }

    private void failAll(Throwable cause) {
        for (Integer id : inFlight.keySet()) {
            CompletableFuture<KafkaFrame> f = inFlight.remove(id);
            if (f != null) {
                f.completeExceptionally(cause);
            }
        }
    }

    @Override
    public void close() {
        channel.close().awaitUninterruptibly(2_000);
    }
}
