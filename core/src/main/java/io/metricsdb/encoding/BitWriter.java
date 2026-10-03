package io.metricsdb.encoding;

import java.util.Arrays;

/** Append-only bit stream, most significant bit first, backed by a growable byte array. */
public final class BitWriter {
    private byte[] buf;
    private int bitLen;

    public BitWriter(int initialBytes) {
        buf = new byte[Math.max(8, initialBytes)];
    }

    public void writeBit(boolean bit) {
        int byteIdx = bitLen >>> 3;
        ensure(byteIdx);
        if (bit) buf[byteIdx] |= (byte) (0x80 >>> (bitLen & 7));
        bitLen++;
    }

    /** Writes the low {@code n} bits of {@code v}, n in [0, 64]. */
    public void writeBits(long v, int n) {
        while (n > 0) {
            int byteIdx = bitLen >>> 3;
            ensure(byteIdx);
            int bitOff = bitLen & 7;
            int free = 8 - bitOff;
            int take = Math.min(free, n);
            int bits = (int) ((v >>> (n - take)) & ((1 << take) - 1));
            buf[byteIdx] |= (byte) (bits << (free - take));
            bitLen += take;
            n -= take;
        }
    }

    private void ensure(int byteIdx) {
        if (byteIdx >= buf.length) buf = Arrays.copyOf(buf, Math.max(buf.length * 2, byteIdx + 8));
    }

    public int bitLength() { return bitLen; }
    public int byteLength() { return (bitLen + 7) >>> 3; }

    /** Direct access for in-place header patching; valid up to {@link #byteLength()}. */
    byte[] buffer() { return buf; }

    public byte[] toByteArray() { return Arrays.copyOf(buf, byteLength()); }
}
