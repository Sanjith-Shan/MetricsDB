package io.metricsdb.storage;

import io.metricsdb.encoding.XorChunk;

import java.nio.ByteBuffer;

/**
 * One encoded Gorilla chunk with its time bounds. The bytes live either on the heap (head
 * chunks, network transfers) or in a memory-mapped block file; the record only references them.
 */
public record Chunk(long minT, long maxT, ByteBuffer buf, int off, int len) {
    public static Chunk of(long minT, long maxT, byte[] data) {
        return new Chunk(minT, maxT, ByteBuffer.wrap(data), 0, data.length);
    }

    public XorChunk.Iterator iterator() { return new XorChunk.Iterator(buf, off, len); }

    public int count() { return XorChunk.count(buf, off); }

    public byte[] bytes() {
        byte[] out = new byte[len];
        buf.get(off, out, 0, len);
        return out;
    }

    public boolean overlaps(long mint, long maxt) { return minT <= maxt && maxT >= mint; }
}
