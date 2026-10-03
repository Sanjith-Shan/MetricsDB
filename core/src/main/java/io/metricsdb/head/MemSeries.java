package io.metricsdb.head;

import io.metricsdb.encoding.XorChunk;
import io.metricsdb.model.Labels;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.SampleArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The in-memory (head) part of one series: sealed Gorilla chunks, one open chunk being appended
 * to, and a small sorted buffer for samples that arrive out of order. A chunk is sealed at
 * {@link XorChunk#MAX_SAMPLES} samples or when a sample crosses a block-range boundary, so every
 * chunk lies inside exactly one block range and can be moved to a block without re-encoding.
 */
public final class MemSeries {
    public enum Result { OK, OOO, DUPLICATE, TOO_OLD, OOO_FULL }

    public static final int MAX_OOO = 1 << 16;

    public final int id;
    public final Labels labels;
    private final ArrayList<Chunk> sealed = new ArrayList<>(2);
    private XorChunk.Appender open;
    private long openBoundary;
    private long maxT = Long.MIN_VALUE;
    private long[] oooT;
    private double[] oooV;
    private int oooN;

    public MemSeries(int id, Labels labels) {
        this.id = id;
        this.labels = labels;
    }

    public synchronized long maxT() { return maxT; }

    /**
     * @param minValidT samples older than this are already in a block and are refused
     * @param oooWindow how far behind this series' newest sample a late sample may be;
     *                  negative means unlimited (WAL replay)
     */
    public synchronized Result append(long t, double v, long minValidT, long oooWindow, long blockRange) {
        if (t < minValidT) return Result.TOO_OLD;
        if (t > maxT) {
            if (open == null || open.count() >= XorChunk.MAX_SAMPLES || t >= openBoundary) {
                seal();
                open = new XorChunk.Appender();
                openBoundary = Math.floorDiv(t, blockRange) * blockRange + blockRange;
            }
            open.append(t, v);
            maxT = t;
            return Result.OK;
        }
        if (t == maxT) return Result.DUPLICATE;
        if (oooWindow >= 0 && t < maxT - oooWindow) return Result.TOO_OLD;
        return insertOoo(t, v);
    }

    private Result insertOoo(long t, double v) {
        if (oooT == null) {
            oooT = new long[8];
            oooV = new double[8];
        }
        int pos = Arrays.binarySearch(oooT, 0, oooN, t);
        if (pos >= 0) return Result.DUPLICATE;
        if (oooN >= MAX_OOO) return Result.OOO_FULL;
        pos = -pos - 1;
        if (oooN == oooT.length) {
            oooT = Arrays.copyOf(oooT, oooN * 2);
            oooV = Arrays.copyOf(oooV, oooN * 2);
        }
        System.arraycopy(oooT, pos, oooT, pos + 1, oooN - pos);
        System.arraycopy(oooV, pos, oooV, pos + 1, oooN - pos);
        oooT[pos] = t;
        oooV[pos] = v;
        oooN++;
        return Result.OOO;
    }

    private void seal() {
        if (open != null && open.count() > 0) sealed.add(Chunk.of(open.minT(), open.maxT(), open.toBytes()));
        open = null;
    }

    /** Head chunks overlapping [mint, maxt]: sealed, a snapshot of the open one, and the late samples. */
    public synchronized void chunks(long mint, long maxt, List<Chunk> out) {
        for (Chunk c : sealed) if (c.overlaps(mint, maxt)) out.add(c);
        if (open != null && open.count() > 0 && open.minT() <= maxt && open.maxT() >= mint) {
            out.add(Chunk.of(open.minT(), open.maxT(), open.toBytes()));
        }
        if (oooN > 0) {
            int from = lowerBound(oooT, oooN, mint);
            int to = lowerBound(oooT, oooN, maxt + 1);
            if (to > from) out.add(Chunk.of(oooT[from], oooT[to - 1], XorChunk.encode(oooT, oooV, from, to)));
        }
    }

    public synchronized boolean isEmpty() {
        return sealed.isEmpty() && (open == null || open.count() == 0) && oooN == 0;
    }

    public synchronized long sampleCount() {
        long n = oooN;
        for (Chunk c : sealed) n += c.count();
        if (open != null) n += open.count();
        return n;
    }

    public synchronized long chunkBytes() {
        long n = 0;
        for (Chunk c : sealed) n += c.len();
        if (open != null) n += open.byteLength();
        return n + oooN * 16L;
    }

    /**
     * Everything older than {@code end}, ready for a block: chunks moved as they are when no late
     * samples fall in the range, otherwise merged and re-encoded.
     */
    public synchronized List<Chunk> chunksBefore(long end) {
        List<Chunk> out = new ArrayList<>();
        for (Chunk c : sealed) if (c.maxT() < end) out.add(c);
        if (open != null && open.count() > 0 && open.maxT() < end) out.add(Chunk.of(open.minT(), open.maxT(), open.toBytes()));
        int lateTo = oooN > 0 ? lowerBound(oooT, oooN, end) : 0;
        if (lateTo == 0) return out;
        List<Chunk> all = new ArrayList<>(out);
        all.add(Chunk.of(oooT[0], oooT[lateTo - 1], XorChunk.encode(oooT, oooV, 0, lateTo)));
        SampleArray merged = SampleArray.decode(all, Long.MIN_VALUE, end - 1, null);
        List<Chunk> rechunked = new ArrayList<>();
        for (int i = 0; i < merged.n; i += XorChunk.MAX_SAMPLES) {
            int to = Math.min(merged.n, i + XorChunk.MAX_SAMPLES);
            rechunked.add(Chunk.of(merged.t[i], merged.t[to - 1], XorChunk.encode(merged.t, merged.v, i, to)));
        }
        return rechunked;
    }

    /** Drops everything older than {@code end} once it is safely in a block. */
    public synchronized void truncateBefore(long end) {
        sealed.removeIf(c -> c.maxT() < end);
        if (open != null && open.count() > 0 && open.maxT() < end) open = null;
        if (oooN > 0) {
            int k = lowerBound(oooT, oooN, end);
            if (k > 0) {
                System.arraycopy(oooT, k, oooT, 0, oooN - k);
                System.arraycopy(oooV, k, oooV, 0, oooN - k);
                oooN -= k;
            }
        }
    }

    public synchronized int oooCount() { return oooN; }

    private static int lowerBound(long[] a, int n, long x) {
        int idx = Arrays.binarySearch(a, 0, n, x);
        if (idx < 0) return -idx - 1;
        while (idx > 0 && a[idx - 1] == x) idx--;
        return idx;
    }
}
