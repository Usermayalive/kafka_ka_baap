package com.simplekafka.tiered;

import com.github.luben.zstd.Zstd;

import java.io.IOException;

/** Zstd compression for segment files in remote storage. Frames carry their content size. */
public final class SegmentCodec {
    /** Level 3 is Zstd's default: ~3-4x on typical record payloads at hundreds of MB/s. */
    public static final int LEVEL = 3;

    private SegmentCodec() {
    }

    public static byte[] compress(byte[] raw) {
        return Zstd.compress(raw, LEVEL);
    }

    public static byte[] decompress(byte[] compressed) throws IOException {
        long size = Zstd.getFrameContentSize(compressed);
        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Corrupt or unsized Zstd segment frame (content size " + size + ")");
        }
        return Zstd.decompress(compressed, (int) size);
    }
}
