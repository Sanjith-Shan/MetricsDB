package io.metricsdb.cluster;

import io.metricsdb.block.Rollup;
import io.metricsdb.model.Labels;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.RollupSeries;
import io.metricsdb.storage.SeriesChunks;
import io.metricsdb.storage.WriteBatch;
import io.metricsdb.util.ByteIn;
import io.metricsdb.util.ByteOut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary formats between the router and storage nodes. Selections travel as the stored Gorilla
 * chunks themselves, so a fan-out query moves compressed bytes, not decoded samples.
 */
public final class Wire {
    private Wire() {}

    private static void labels(ByteOut o, Labels l) {
        o.uvarint(l.size());
        for (int i = 0; i < l.size(); i++) o.str(l.name(i)).str(l.value(i));
    }

    private static Labels labels(ByteIn in, Map<String, String> intern) {
        int n = (int) in.uvarint();
        String[] kv = new String[n * 2];
        for (int i = 0; i < kv.length; i++) {
            String s = in.str();
            String prev = intern.putIfAbsent(s, s);
            kv[i] = prev != null ? prev : s;
        }
        return Labels.fromSorted(kv);
    }

    private static void chunks(ByteOut o, List<Chunk> cs) {
        o.uvarint(cs.size());
        for (Chunk c : cs) {
            o.varint(c.minT()).varint(c.maxT()).uvarint(c.len());
            byte[] b = new byte[c.len()];
            c.buf().get(c.off(), b, 0, c.len());
            o.bytes(b);
        }
    }

    private static List<Chunk> chunks(ByteIn in, byte[] src) {
        int n = (int) in.uvarint();
        List<Chunk> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long mint = in.varint(), maxt = in.varint();
            int len = (int) in.uvarint();
            out.add(new Chunk(mint, maxt, java.nio.ByteBuffer.wrap(src), in.position(), len));
            in.skip(len);
        }
        return out;
    }

    // ------------------------------------------------------------- write batches

    public static byte[] encodeBatch(WriteBatch b, int[] idx, int n) {
        ByteOut o = new ByteOut(64 + n * 24);
        o.uvarint(n);
        Labels prev = null;
        for (int k = 0; k < n; k++) {
            int i = idx == null ? k : idx[k];
            Labels l = b.labels[i];
            if (l == prev) {
                o.u8(0);
            } else {
                o.u8(1);
                labels(o, l);
                prev = l;
            }
            o.varint(b.t[i]).i64(Double.doubleToRawLongBits(b.v[i]));
        }
        return o.toByteArray();
    }

    public static WriteBatch decodeBatch(byte[] data) {
        ByteIn in = new ByteIn(data);
        int n = (int) in.uvarint();
        WriteBatch b = new WriteBatch(n);
        Labels cur = null;
        Map<String, String> intern = new HashMap<>();
        for (int k = 0; k < n; k++) {
            if (in.u8() == 1) cur = labels(in, intern);
            long t = in.varint();
            double v = Double.longBitsToDouble(in.i64());
            b.add(cur, t, v);
        }
        return b;
    }

    // ------------------------------------------------------------- selections

    public static byte[] encodeSeries(List<SeriesChunks> series) {
        ByteOut o = new ByteOut(1 << 14);
        o.uvarint(series.size());
        for (SeriesChunks s : series) {
            labels(o, s.labels());
            chunks(o, s.chunks());
        }
        return o.toByteArray();
    }

    public static List<SeriesChunks> decodeSeries(byte[] data) {
        ByteIn in = new ByteIn(data);
        int n = (int) in.uvarint();
        Map<String, String> intern = new HashMap<>();
        List<SeriesChunks> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(new SeriesChunks(labels(in, intern), chunks(in, data)));
        return out;
    }

    public static byte[] encodeRollups(List<RollupSeries> series) {
        ByteOut o = new ByteOut(1 << 14);
        o.uvarint(series.size());
        for (RollupSeries s : series) {
            labels(o, s.labels());
            for (int k = 0; k < Rollup.AGGS; k++) chunks(o, s.aggs()[k]);
        }
        return o.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public static List<RollupSeries> decodeRollups(byte[] data) {
        ByteIn in = new ByteIn(data);
        int n = (int) in.uvarint();
        Map<String, String> intern = new HashMap<>();
        List<RollupSeries> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Labels l = labels(in, intern);
            List<Chunk>[] aggs = new List[Rollup.AGGS];
            for (int k = 0; k < Rollup.AGGS; k++) aggs[k] = chunks(in, data);
            out.add(new RollupSeries(l, aggs));
        }
        return out;
    }

    // ------------------------------------------------------------- matchers

    public static String encodeMatchers(List<io.metricsdb.index.Matcher> ms) {
        StringBuilder sb = new StringBuilder();
        for (var m : ms) {
            if (!sb.isEmpty()) sb.append('\u0001');
            sb.append(m.type.name()).append('\u0002').append(m.name).append('\u0002').append(m.value);
        }
        return sb.toString();
    }

    public static List<io.metricsdb.index.Matcher> decodeMatchers(String s) {
        List<io.metricsdb.index.Matcher> out = new ArrayList<>();
        if (s.isEmpty()) return out;
        for (String part : s.split("\u0001")) {
            String[] f = part.split("\u0002", 3);
            out.add(new io.metricsdb.index.Matcher(io.metricsdb.index.Matcher.Type.valueOf(f[0]), f[1], f[2]));
        }
        return out;
    }
}
