package io.metricsdb.encoding;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XorChunkTest {

    record Series(long[] t, double[] v) {}

    @Provide
    Arbitrary<Series> series() {
        Arbitrary<Integer> sizes = Arbitraries.integers().between(1, 400);
        Arbitrary<Long> starts = Arbitraries.longs().between(-1L << 50, 1L << 50);
        Arbitrary<Integer> modes = Arbitraries.integers().between(0, 3);
        return Combinators.combine(sizes, starts, modes, Arbitraries.longs()).as((n, start, mode, seed) -> {
            java.util.Random r = new java.util.Random(seed);
            long[] t = new long[n];
            double[] v = new double[n];
            long cur = start;
            double val = r.nextGaussian() * 100;
            for (int i = 0; i < n; i++) {
                // steady intervals with jitter, occasional gaps, and huge jumps
                long step = switch (r.nextInt(10)) {
                    case 0 -> 1 + r.nextInt(5);
                    case 1 -> 1 + (long) r.nextInt(1 << 30);
                    case 2 -> 1 + (Math.abs(r.nextLong()) >>> 20);
                    default -> 10_000 + r.nextInt(21) - 10;
                };
                cur += step;
                t[i] = cur;
                val = switch (mode) {
                    case 0 -> val + r.nextGaussian();                       // random walk
                    case 1 -> Math.rint(val + r.nextInt(5) - 2);            // integer gauge
                    case 2 -> r.nextInt(8) == 0 ? val : r.nextDouble();     // repeats and noise
                    default -> specials(r);                                 // NaN, infinities, signed zero
                };
                v[i] = val;
            }
            return new Series(t, v);
        });
    }

    private static double specials(java.util.Random r) {
        return switch (r.nextInt(8)) {
            case 0 -> Double.NaN;
            case 1 -> Double.longBitsToDouble(0x7ff0000000000001L); // NaN with a payload (Prometheus stale marker style)
            case 2 -> Double.POSITIVE_INFINITY;
            case 3 -> Double.NEGATIVE_INFINITY;
            case 4 -> -0.0;
            case 5 -> Double.MIN_VALUE;
            case 6 -> Double.MAX_VALUE;
            default -> r.nextLong() * 1.0;
        };
    }

    @Property(tries = 500)
    void roundTripsEverySampleBitExact(@ForAll("series") Series s) {
        XorChunk.Appender a = new XorChunk.Appender();
        for (int i = 0; i < s.t.length; i++) a.append(s.t[i], s.v[i]);
        byte[] bytes = a.toBytes();
        XorChunk.Iterator it = new XorChunk.Iterator(bytes);
        assertEquals(s.t.length, it.total());
        for (int i = 0; i < s.t.length; i++) {
            assertTrue(it.next());
            assertEquals(s.t[i], it.t(), "timestamp " + i);
            assertEquals(Double.doubleToRawLongBits(s.v[i]), it.vBits(), "value bits " + i);
        }
        assertFalse(it.next());
    }

    @Property(tries = 500)
    void bestEncodingRoundTripsBitExact(@ForAll("series") Series s, @ForAll boolean integerise) {
        double[] v = s.v.clone();
        if (integerise) for (int i = 0; i < v.length; i++) v[i] = Double.isFinite(v[i]) ? Math.rint(v[i]) : v[i];
        byte[] bytes = XorChunk.encodeBest(s.t, v, 0, v.length);
        XorChunk.Iterator it = new XorChunk.Iterator(bytes);
        for (int i = 0; i < v.length; i++) {
            assertTrue(it.next());
            assertEquals(s.t[i], it.t());
            assertEquals(Double.doubleToRawLongBits(v[i]), it.vBits(), "value bits " + i + " of " + v[i]);
        }
        assertFalse(it.next());
    }

    @Test
    void integerEncodingIsUsedOnlyWhenExact() {
        long[] t = {1000, 2000, 3000};
        assertTrue((XorChunk.encodeBest(t, new double[]{1, 5, -3}, 0, 3)[0] & 0x80) != 0);
        assertFalse((XorChunk.encodeBest(t, new double[]{1, -0.0, 2}, 0, 3)[0] & 0x80) != 0, "-0.0 must keep its sign bit");
        assertFalse((XorChunk.encodeBest(t, new double[]{1, 0.5, 2}, 0, 3)[0] & 0x80) != 0);
        assertFalse((XorChunk.encodeBest(t, new double[]{1, 1e300, 2}, 0, 3)[0] & 0x80) != 0);
    }

    @Property(tries = 200)
    void snapshotWhileAppendingIsConsistent(@ForAll("series") Series s, @ForAll @IntRange(min = 0, max = 400) int cut) {
        XorChunk.Appender a = new XorChunk.Appender();
        int k = Math.min(cut, s.t.length);
        for (int i = 0; i < k; i++) a.append(s.t[i], s.v[i]);
        byte[] snap = a.toBytes();
        for (int i = k; i < s.t.length; i++) a.append(s.t[i], s.v[i]);
        XorChunk.Iterator it = new XorChunk.Iterator(snap);
        int n = 0;
        while (it.next()) {
            assertEquals(s.t[n], it.t());
            n++;
        }
        assertEquals(k, n);
    }

    @Test
    void regularMetricsCompressWell() {
        // 10 s scrape interval, slowly changing integer gauge: the common case Gorilla targets
        XorChunk.Appender a = new XorChunk.Appender();
        long t = 1_700_000_000_000L;
        double v = 42;
        for (int i = 0; i < 120; i++) {
            a.append(t, v);
            t += 10_000;
            if (i % 10 == 0) v++;
        }
        double bytesPerSample = a.toBytes().length / 120.0;
        assertTrue(bytesPerSample < 1.0, "bytes per sample " + bytesPerSample);
    }

    @Test
    void bitWriterAndReaderAgreeOnWidths() {
        BitWriter w = new BitWriter(8);
        List<long[]> written = new java.util.ArrayList<>();
        java.util.Random r = new java.util.Random(7);
        for (int i = 0; i < 2000; i++) {
            int n = r.nextInt(65);
            long v = n == 64 ? r.nextLong() : (n == 0 ? 0 : r.nextLong() & ((1L << n) - 1));
            w.writeBits(v, n);
            written.add(new long[]{v, n});
        }
        BitReader rd = new BitReader(java.nio.ByteBuffer.wrap(w.toByteArray()), 0, w.byteLength());
        for (long[] x : written) assertEquals(x[0], rd.readBits((int) x[1]));
    }
}
