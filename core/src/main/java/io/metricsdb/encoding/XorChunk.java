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

    /** High bit of the 2-byte header: values are integer deltas instead of XORed floats. */
    public static final int INT_FLAG = 0x8000;
    /** Second header bit (integer chunks): values store delta-of-deltas, which suits counters. */
    public static final int DOD_FLAG = 0x4000;
    public static final int MAX_COUNT = 0x3fff;

    public static int count(ByteBuffer b, int offset) {
        return (((b.get(offset) & 0xff) << 8) | (b.get(offset + 1) & 0xff)) & MAX_COUNT;
    }

    public static boolean isInteger(ByteBuffer b, int offset) {
        return (b.get(offset) & 0x80) != 0;
    }

    public static int count(byte[] chunk) {
        return (((chunk[0] & 0xff) << 8) | (chunk[1] & 0xff)) & MAX_COUNT;
    }

    /** Encodes sorted, de-duplicated samples into one chunk (any length up to 65535). */
    public static byte[] encode(long[] ts, double[] vs, int from, int to) {
        Appender a = new Appender();
        for (int i = from; i < to; i++) a.append(ts[i], vs[i]);
        return a.toBytes();
    }

    /**
     * Encodes with the integer variant when every value is an exact integer (counters, gauges of
     * counts and percentages), otherwise with Gorilla XOR. Both decode bit for bit to the input.
     */
    public static byte[] encodeBest(long[] ts, double[] vs, int from, int to) {
        for (int i = from; i < to; i++) if (!exactLong(vs[i])) return encode(ts, vs, from, to);
        byte[] delta = encodeInt(ts, vs, from, to, false);
        byte[] dod = encodeInt(ts, vs, from, to, true);
        return dod.length < delta.length ? dod : delta;
    }

    /** True when v converts to a long and back with the same bits (rules out -0.0, NaN, fractions). */
    static boolean exactLong(double v) {
        if (!(Math.abs(v) <= (1L << 53))) return false;
        long l = (long) v;
        return Double.doubleToRawLongBits((double) l) == Double.doubleToRawLongBits(v);
    }

    /**
     * Integer variant: timestamps exactly as in Gorilla; values as the difference from the
     * previous integer in 1, 2+4, 3+8, 4+16, 5+32 or 5+64 bits. A gauge that moves by a few units
     * per sample costs 6 bits instead of the 13 or more an XOR with a new window takes. With
     * {@code dodValues} the values store the change of the difference instead, so a counter that
     * grows by about the same amount each interval costs the same few bits as a flat gauge.
     */
    static byte[] encodeInt(long[] ts, double[] vs, int from, int to, boolean dodValues) {
        int n = to - from;
        if (n > MAX_COUNT) throw new IllegalStateException("chunk over " + MAX_COUNT + " samples");
        BitWriter w = new BitWriter(16 + n);
        w.writeBits(INT_FLAG | (dodValues ? DOD_FLAG : 0) | n, 16);
        long prevT = 0, prevDelta = 0, prevV = 0, prevD = 0;
        for (int i = from; i < to; i++) {
            long t = ts[i], v = (long) vs[i];
            if (i == from) {
                w.writeBits(t, 64);
                w.writeBits(v, 64);
            } else {
                long delta = t - prevT;
                writeDod(w, i == from + 1 ? delta : delta - prevDelta);
                prevDelta = delta;
                long d = v - prevV;
                if (dodValues) {
                    long dd = i == from + 1 ? d : d - prevD;
                    prevD = d;
                    d = dd;
                }
                if (d == 0) w.writeBit(false);
                else if (fits(d, 4)) { w.writeBits(0b10, 2); w.writeBits(d, 4); }
                else if (fits(d, 8)) { w.writeBits(0b110, 3); w.writeBits(d, 8); }
                else if (fits(d, 16)) { w.writeBits(0b1110, 4); w.writeBits(d, 16); }
                else if (fits(d, 32)) { w.writeBits(0b11110, 5); w.writeBits(d, 32); }
                else { w.writeBits(0b11111, 5); w.writeBits(d, 64); }
            }
            prevT = t;
            prevV = v;
        }
        return w.toByteArray();
    }

    static void writeDod(BitWriter w, long dod) {
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

    static boolean fits(long v, int bits) {
        long lim = 1L << (bits - 1);
        return v >= -lim && v < lim;
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
            if (++count > MAX_COUNT) throw new IllegalStateException("chunk over " + MAX_COUNT + " samples");
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
        private final boolean integer;
        private long iv, ivDelta;
        private final boolean dodValues;

        public Iterator(ByteBuffer b, int offset, int length) {
            this.total = count(b, offset);
            this.integer = isInteger(b, offset);
            this.dodValues = integer && (b.get(offset) & 0x40) != 0;
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
                if (integer) { iv = vBits; vBits = Double.doubleToRawLongBits((double) iv); }
            } else {
                long dod = readDod();
                delta = read == 1 ? dod : delta + dod;
                t += delta;
                if (integer) {
                    long x = readIntDelta();
                    if (dodValues) {
                        ivDelta = read == 1 ? x : ivDelta + x;
                        x = ivDelta;
                    }
                    iv += x;
                    vBits = Double.doubleToRawLongBits((double) iv);
                } else {
                    readXor();
                }
            }
            read++;
            return true;
        }

        private long readDod() {
            return switch (r.readUnary(4)) {
                case 0 -> 0;
                case 1 -> signExtend(r.readBits(14), 14);
                case 2 -> signExtend(r.readBits(17), 17);
                case 3 -> signExtend(r.readBits(20), 20);
                default -> r.readBits(64);
            };
        }

        private static long signExtend(long v, int bits) {
            return (v << (64 - bits)) >> (64 - bits);
        }

        private long readIntDelta() {
            return switch (r.readUnary(5)) {
                case 0 -> 0;
                case 1 -> signExtend(r.readBits(4), 4);
                case 2 -> signExtend(r.readBits(8), 8);
                case 3 -> signExtend(r.readBits(16), 16);
                case 4 -> signExtend(r.readBits(32), 32);
                default -> r.readBits(64);
            };
        }

        private void readXor() {
            int c = r.readUnary(2);
            if (c == 0) return;
            if (c == 2) {
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
