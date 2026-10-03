package io.metricsdb.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Growable byte buffer with varint and string helpers, used by the WAL, block files and the wire format. */
public final class ByteOut {
    private byte[] buf;
    private int len;

    public ByteOut() { this(256); }
    public ByteOut(int cap) { buf = new byte[Math.max(16, cap)]; }

    private void ensure(int extra) {
        if (len + extra > buf.length) buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + extra));
    }

    public ByteOut u8(int v) { ensure(1); buf[len++] = (byte) v; return this; }

    public ByteOut i32(int v) {
        ensure(4);
        buf[len++] = (byte) (v >>> 24); buf[len++] = (byte) (v >>> 16);
        buf[len++] = (byte) (v >>> 8); buf[len++] = (byte) v;
        return this;
    }

    public ByteOut i64(long v) {
        ensure(8);
        for (int s = 56; s >= 0; s -= 8) buf[len++] = (byte) (v >>> s);
        return this;
    }

    public ByteOut uvarint(long v) {
        ensure(10);
        while ((v & ~0x7FL) != 0) { buf[len++] = (byte) ((v & 0x7F) | 0x80); v >>>= 7; }
        buf[len++] = (byte) v;
        return this;
    }

    public ByteOut varint(long v) { return uvarint((v << 1) ^ (v >> 63)); }

    public ByteOut bytes(byte[] b) { return bytes(b, 0, b.length); }

    public ByteOut bytes(byte[] b, int off, int n) {
        ensure(n);
        System.arraycopy(b, off, buf, len, n);
        len += n;
        return this;
    }

    public ByteOut lenBytes(byte[] b) { uvarint(b.length); return bytes(b); }

    public ByteOut str(String s) { return lenBytes(s.getBytes(StandardCharsets.UTF_8)); }

    public void setI32(int at, int v) {
        buf[at] = (byte) (v >>> 24); buf[at + 1] = (byte) (v >>> 16);
        buf[at + 2] = (byte) (v >>> 8); buf[at + 3] = (byte) v;
    }

    public int length() { return len; }
    public byte[] array() { return buf; }
    public byte[] toByteArray() { return Arrays.copyOf(buf, len); }
    public void reset() { len = 0; }
}
