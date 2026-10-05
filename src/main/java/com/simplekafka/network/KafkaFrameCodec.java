package com.simplekafka.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.util.List;

/**
 * Netty codec transforming incoming ByteBuf into KafkaFrame and vice versa.
 * Used in conjunction with LengthFieldBasedFrameDecoder and LengthFieldPrepender.
 */
public class KafkaFrameCodec extends ByteToMessageCodec<KafkaFrame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, KafkaFrame frame, ByteBuf out) throws Exception {
        frame.encode(out);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 8) { // 2 + 2 + 4 = 8 bytes minimum header
            return;
        }
        KafkaFrame frame = KafkaFrame.decode(in);
        out.add(frame);
    }
}
