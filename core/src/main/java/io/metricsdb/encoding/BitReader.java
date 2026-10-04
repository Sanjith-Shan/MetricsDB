package io.metricsdb.encoding;

import java.nio.ByteBuffer;

/**
 * Reads a most-significant-bit-first stream from a region of a {@link ByteBuffer} (heap or
 * memory-mapped) using absolute gets. Bits are buffered left-aligned in a long, refilled eight
 * bytes at a time where possible, and the variable-length control prefixes of the chunk formats
 * are read with one leading-ones count rather than bit by bit.
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
        if (avail > 56) return;
        if (pos + 8 <= end) {
            int bytes = (64 - avail) >>> 3;
            long word = b.getLong(pos); // big-endian: the next bytes, most significant first
            int newAvail = avail + (bytes << 3);
            buf |= word >>> avail;
            if (newAvail < 64) buf &= ~(-1L >>> newAvail);
            avail = newAvail;
            pos += bytes;
            return;
        }
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

    /**
     * Reads a unary prefix: the number of 1 bits before a 0, consuming the 0, or {@code max} when
     * {@code max} ones are read (then no terminating 0 is consumed). {@code max} is at most 8.
     */
    public int readUnary(int max) {
        if (avail <= max) refill();
        int ones = Long.numberOfLeadingZeros(~buf);
        if (ones >= max) {
            if (avail < max) throw new IllegalStateException("bit stream exhausted");
            buf <<= max;
            avail -= max;
            return max;
        }
        if (avail < ones + 1) throw new IllegalStateException("bit stream exhausted");
        buf <<= ones + 1;
        avail -= ones + 1;
        return ones;
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
