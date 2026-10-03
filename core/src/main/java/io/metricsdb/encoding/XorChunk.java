package io.metricsdb.encoding;

import java.nio.ByteBuffer;

/**
 * Gorilla compression (Pelkonen et al., VLDB 2015) for one chunk of a series.
 *
 * <p>Layout: a 2-byte big-endian sample count, then a bit stream. The first sample stores its
 * timestamp and value raw (64 bits each). Every later timestamp stores the delta of deltas in
 * one of five buckets (1, 2+14, 3+17, 4+20 or 4+64 bits; the paper's 7/9/12/32-bit buckets
 * assume second precision, millisecond timestamps need wider ones). Every later value stores
 * the XOR with the previous value: a single 0 bit when equal, otherwise the meaningful bits,
 * reusing the previous leading/trailing-zero window when it still fits.
 */
public final class XorChunk {
    public static final int MAX_SAMPLES = 120;
    public static final int HEADER_BYTES = 2;

    private XorChunk() {}

    public static int count(ByteBuffer b, int offset) {
        return ((b.get(offset) & 0xff) << 8) | (b.get(offset + 1) & 0xff);
    }

    public static int count(byte[] chunk) {
        return ((chunk[0] & 0xff) << 8) | (chunk[1] & 0xff);
    }

    /** Encodes sorted, de-duplicated samples into one chunk (any length up to 65535). */
    public static byte[] encode(long[] ts, double[] vs, int from, int to) {
        Appender a = new Appender();
        for (int i = from; i < to; i++) a.append(ts[i], vs[i]);
        return a.toBytes();
    }

    public static final class Appender {
        private final BitWriter w = new BitWriter(64);
        private int count;
        private long prevT;
        private long prevDelta;
        private long prevV;
        private int leading = 0xff;
        private int trailing;
        private long minT, maxT;

        public Appender() {
            w.writeBits(0, 16); // count, patched in toBytes()
        }

        public void append(long t, double v) {
            long vBits = Double.doubleToRawLongBits(v);
            if (count == 0) {
                w.writeBits(t, 64);
                w.writeBits(vBits, 64);
                minT = t;
            } else {
                long delta = t - prevT;
                writeDod(count == 1 ? delta : delta - prevDelta);
                prevDelta = delta;
                writeXor(vBits);
            }
            prevT = t;
            prevV = vBits;
            maxT = t;
            if (++count > 0xffff) throw new IllegalStateException("chunk over 65535 samples");
        }

        private void writeDod(long dod) {
            if (dod == 0) {
                w.writeBit(false);
            } else if (fits(dod, 14)) {
                w.writeBits(0b10, 2);
                w.writeBits(dod, 14);
            } else if (fits(dod, 17)) {
                w.writeBits(0b110, 3);
                w.writeBits(dod, 17);
            } else if (fits(dod, 20)) {
                w.writeBits(0b1110, 4);
                w.writeBits(dod, 20);
            } else {
                w.writeBits(0b1111, 4);
                w.writeBits(dod, 64);
            }
        }

        private static boolean fits(long v, int bits) {
            long lim = 1L << (bits - 1);
            return v >= -lim && v < lim;
        }

        private void writeXor(long vBits) {
            long x = vBits ^ prevV;
            if (x == 0) {
                w.writeBit(false);
                return;
            }
            w.writeBit(true);
            int lead = Math.min(31, Long.numberOfLeadingZeros(x));
            int trail = Long.numberOfTrailingZeros(x);
            if (leading != 0xff && lead >= leading && trail >= trailing) {
                w.writeBit(false);
                w.writeBits(x >>> trailing, 64 - leading - trailing);
            } else {
                leading = lead;
                trailing = trail;
                int sig = 64 - lead - trail;
                w.writeBit(true);
                w.writeBits(lead, 5);
                w.writeBits(sig == 64 ? 0 : sig, 6);
                w.writeBits(x >>> trail, sig);
            }
        }

        public int count() { return count; }
        public long minT() { return minT; }
        public long maxT() { return maxT; }
        public long lastT() { return prevT; }
        public int byteLength() { return w.byteLength(); }

        /** A self-contained snapshot; the appender stays usable. */
        public byte[] toBytes() {
            byte[] out = w.toByteArray();
            out[0] = (byte) (count >>> 8);
            out[1] = (byte) count;
            return out;
        }
    }

    /** Iterates the samples of one encoded chunk held in a heap or mapped buffer. */
    public static final class Iterator {
        private final BitReader r;
        private final int total;
        private int read;
        private long t, delta;
        private long vBits;
        private int leading, trailing;

        public Iterator(ByteBuffer b, int offset, int length) {
            this.total = count(b, offset);
            this.r = new BitReader(b, offset + HEADER_BYTES, length - HEADER_BYTES);
        }

        public Iterator(byte[] chunk) {
            this(ByteBuffer.wrap(chunk), 0, chunk.length);
        }

        public int total() { return total; }

        public boolean next() {
            if (read >= total) return false;
            if (read == 0) {
                t = r.readBits(64);
                vBits = r.readBits(64);
            } else {
                long dod = readDod();
                delta = read == 1 ? dod : delta + dod;
                t += delta;
                readXor();
            }
            read++;
            return true;
        }

        private long readDod() {
            if (!r.readBit()) return 0;
            if (!r.readBit()) return signExtend(r.readBits(14), 14);
            if (!r.readBit()) return signExtend(r.readBits(17), 17);
            if (!r.readBit()) return signExtend(r.readBits(20), 20);
            return r.readBits(64);
        }

        private static long signExtend(long v, int bits) {
            return (v << (64 - bits)) >> (64 - bits);
        }

        private void readXor() {
            if (!r.readBit()) return;
            if (r.readBit()) {
                leading = (int) r.readBits(5);
                int sig = (int) r.readBits(6);
                if (sig == 0) sig = 64;
                trailing = 64 - leading - sig;
            }
            int sig = 64 - leading - trailing;
            vBits ^= r.readBits(sig) << trailing;
        }

        public long t() { return t; }
        public double v() { return Double.longBitsToDouble(vBits); }
        public long vBits() { return vBits; }
    }
}
