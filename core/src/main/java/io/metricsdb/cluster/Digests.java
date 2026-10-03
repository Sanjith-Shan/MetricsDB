package io.metricsdb.cluster;

import io.metricsdb.head.MemSeries;
import io.metricsdb.model.Labels;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.SampleArray;
import io.metricsdb.storage.Tsdb;
import io.metricsdb.util.ByteIn;
import io.metricsdb.util.ByteOut;

import java.util.ArrayList;
import java.util.List;

/**
 * Anti-entropy digests, a two-level hash tree. Level 0: one hash per ring range (all of a
 * node's series in that range, all buckets). Level 1: for chosen ranges, one leaf per series per
 * time bucket. Each sample contributes {@code mix(t, value bits)} to a sum, so the hash does not
 * depend on how samples are chunked or in what order they arrived.
 */
public final class Digests {
    private Digests() {}

    public static long mix(long t, long vBits) {
        long z = t * 0x9E3779B97F4A7C15L + vBits;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    public record Request(long mint, long maxt, long bucketMs, long[] tokens, int level, boolean[] ranges) {
        public byte[] encode() {
            ByteOut o = new ByteOut(64 + tokens.length * 8);
            o.varint(mint).varint(maxt).varint(bucketMs).uvarint(tokens.length);
            for (long t : tokens) o.i64(t);
            o.u8(level);
            if (level == 1) for (boolean r : ranges) o.u8(r ? 1 : 0);
            return o.toByteArray();
        }

        public static Request decode(byte[] b) {
            ByteIn in = new ByteIn(b);
            long mint = in.varint(), maxt = in.varint(), bucket = in.varint();
            long[] tokens = new long[(int) in.uvarint()];
            for (int i = 0; i < tokens.length; i++) tokens[i] = in.i64();
            int level = in.u8();
            boolean[] ranges = null;
            if (level == 1) {
                ranges = new boolean[tokens.length];
                for (int i = 0; i < ranges.length; i++) ranges[i] = in.u8() == 1;
            }
            return new Request(mint, maxt, bucket, tokens, level, ranges);
        }
    }

    /** One series' leaves: bucket start times with sample count and hash. */
    public record Leaf(Labels labels, long[] bucket, long[] count, long[] hash) {}

    public static byte[] compute(Tsdb db, Request req) {
        int nr = req.tokens().length;
        long[] rangeHash = new long[nr];
        long[] rangeCount = new long[nr];
        List<Leaf> leaves = new ArrayList<>();
        db.forEachSeries((MemSeries s) -> {
            int r = HashRing.rangeOf(req.tokens(), s.labels.stableHash());
            if (req.level() == 1 && !req.ranges()[r]) return;
            List<Chunk> cs = db.seriesChunks(s, req.mint(), req.maxt());
            if (cs.isEmpty()) return;
            SampleArray sa = SampleArray.decode(cs, req.mint(), req.maxt(), null);
            if (sa.n == 0) return;
            if (req.level() == 0) {
                long h = 0;
                for (int i = 0; i < sa.n; i++) h += mix(sa.t[i], Double.doubleToRawLongBits(sa.v[i]));
                rangeHash[r] += h * 31 + s.labels.stableHash();
                rangeCount[r] += sa.n;
                return;
            }
            List<long[]> bs = new ArrayList<>();
            long curB = Long.MIN_VALUE;
            long[] cur = null;
            for (int i = 0; i < sa.n; i++) {
                long b = Math.floorDiv(sa.t[i], req.bucketMs()) * req.bucketMs();
                if (b != curB) {
                    cur = new long[]{b, 0, 0};
                    bs.add(cur);
                    curB = b;
                }
                cur[1]++;
                cur[2] += mix(sa.t[i], Double.doubleToRawLongBits(sa.v[i]));
            }
            long[] bucket = new long[bs.size()], count = new long[bs.size()], hash = new long[bs.size()];
            for (int i = 0; i < bs.size(); i++) { bucket[i] = bs.get(i)[0]; count[i] = bs.get(i)[1]; hash[i] = bs.get(i)[2]; }
            synchronized (leaves) { leaves.add(new Leaf(s.labels, bucket, count, hash)); }
        });
        ByteOut o = new ByteOut(1 << 12);
        if (req.level() == 0) {
            o.uvarint(nr);
            for (int i = 0; i < nr; i++) o.i64(rangeHash[i]).uvarint(rangeCount[i]);
        } else {
            o.uvarint(leaves.size());
            for (Leaf l : leaves) {
                o.uvarint(l.labels().size());
                for (int i = 0; i < l.labels().size(); i++) o.str(l.labels().name(i)).str(l.labels().value(i));
                o.uvarint(l.bucket().length);
                for (int i = 0; i < l.bucket().length; i++) o.varint(l.bucket()[i]).uvarint(l.count()[i]).i64(l.hash()[i]);
            }
        }
        return o.toByteArray();
    }

    public static long[][] decodeRanges(byte[] b) {
        ByteIn in = new ByteIn(b);
        int n = (int) in.uvarint();
        long[][] out = new long[n][2];
        for (int i = 0; i < n; i++) { out[i][0] = in.i64(); out[i][1] = in.uvarint(); }
        return out;
    }

    public static List<Leaf> decodeLeaves(byte[] b) {
        ByteIn in = new ByteIn(b);
        int n = (int) in.uvarint();
        List<Leaf> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            int nl = (int) in.uvarint();
            String[] kv = new String[nl * 2];
            for (int i = 0; i < kv.length; i++) kv[i] = in.str();
            int nb = (int) in.uvarint();
            long[] bucket = new long[nb], count = new long[nb], hash = new long[nb];
            for (int i = 0; i < nb; i++) { bucket[i] = in.varint(); count[i] = in.uvarint(); hash[i] = in.i64(); }
            out.add(new Leaf(Labels.fromSorted(kv), bucket, count, hash));
        }
        return out;
    }
}
