package io.metricsdb.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Sequential reader over a heap or mapped {@link ByteBuffer} region, mirror of {@link ByteOut}. */
public final class ByteIn {
    private final ByteBuffer b;
    private int pos;
    private final int end;

    public ByteIn(byte[] a) { this(ByteBuffer.wrap(a), 0, a.length); }
    public ByteIn(byte[] a, int off, int len) { this(ByteBuffer.wrap(a), off, len); }

    public ByteIn(ByteBuffer b, int off, int len) {
        this.b = b;
        this.pos = off;
        this.end = off + len;
    }

    public boolean hasMore() { return pos < end; }
    public int position() { return pos; }
    public int remaining() { return end - pos; }
    public ByteBuffer buffer() { return b; }

    private void need(int n) {
        if (pos + n > end) throw new IllegalStateException("truncated input");
    }

    public int u8() { need(1); return b.get(pos++) & 0xff; }

    public int i32() {
        need(4);
        int v = b.getInt(pos);
        pos += 4;
        return v;
    }

    public long i64() {
        need(8);
        long v = b.getLong(pos);
        pos += 8;
        return v;
    }

    public long uvarint() {
        long v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            need(1);
            byte x = b.get(pos++);
            v |= (long) (x & 0x7F) << shift;
            if (x >= 0) return v;
        }
        throw new IllegalStateException("varint too long");
    }

    public long varint() {
        long u = uvarint();
        return (u >>> 1) ^ -(u & 1);
    }

    public byte[] bytes(int n) {
        need(n);
        byte[] out = new byte[n];
        b.get(pos, out, 0, n);
        pos += n;
        return out;
    }

    public byte[] lenBytes() { return bytes((int) uvarint()); }

    public String str() {
        int n = (int) uvarint();
        need(n);
        if (b.hasArray()) {
            String s = new String(b.array(), b.arrayOffset() + pos, n, StandardCharsets.UTF_8);
            pos += n;
            return s;
        }
        return new String(bytes(n), StandardCharsets.UTF_8);
    }

    public void skip(int n) { need(n); pos += n; }
}
