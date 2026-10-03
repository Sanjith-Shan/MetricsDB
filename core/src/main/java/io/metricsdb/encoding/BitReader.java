package io.metricsdb.encoding;

import java.nio.ByteBuffer;

/**
 * Reads a most-significant-bit-first stream from a region of a {@link ByteBuffer} (heap or
 * memory-mapped) using absolute gets, buffering up to 64 bits at a time.
 */
public final class BitReader {
    private final ByteBuffer b;
    private int pos;
    private final int end;
    private long buf;   // left-aligned pending bits
    private int avail;  // number of valid bits in buf

    public BitReader(ByteBuffer b, int offset, int length) {
        this.b = b;
        this.pos = offset;
        this.end = offset + length;
    }

    private void refill() {
        while (avail <= 56 && pos < end) {
            buf |= (long) (b.get(pos++) & 0xff) << (56 - avail);
            avail += 8;
        }
    }

    public boolean readBit() {
        if (avail == 0) {
            refill();
            if (avail == 0) throw new IllegalStateException("bit stream exhausted");
        }
        boolean bit = buf < 0;
        buf <<= 1;
        avail--;
        return bit;
    }

    /** Reads {@code n} bits, n in [0, 64], as an unsigned value in the low bits. */
    public long readBits(int n) {
        if (n == 0) return 0;
        if (n > 56) {
            long hi = readBits(n - 32);
            return (hi << 32) | readBits(32);
        }
        if (avail < n) {
            refill();
            if (avail < n) throw new IllegalStateException("bit stream exhausted");
        }
        long v = buf >>> (64 - n);
        buf <<= n;
        avail -= n;
        return v;
    }
}
