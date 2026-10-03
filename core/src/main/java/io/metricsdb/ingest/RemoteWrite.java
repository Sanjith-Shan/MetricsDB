package io.metricsdb.ingest;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Prometheus remote write 1.0: a snappy-compressed protobuf {@code WriteRequest}. Decoded by
 * hand from the wire format (field 1 of WriteRequest is a TimeSeries, whose field 1 is a Label
 * and field 2 a Sample), so no protobuf runtime is needed. Metadata and exemplars are skipped.
 */
public final class RemoteWrite {
    private RemoteWrite() {}

    public static int decode(byte[] snappyBody, WriteBatch out) {
        byte[] pb = Snappy.decompress(snappyBody);
        return decodeProto(pb, out);
    }

    public static int decodeProto(byte[] b, WriteBatch out) {
        Reader r = new Reader(b, 0, b.length);
        int samples = 0;
        while (r.more()) {
            int key = (int) r.varint();
            if (key >>> 3 == 1 && (key & 7) == 2) {
                int len = (int) r.varint();
                samples += timeSeries(new Reader(b, r.p, r.p + len), out);
                r.p += len;
            } else {
                r.skip(key & 7);
            }
        }
        return samples;
    }

    private static int timeSeries(Reader r, WriteBatch out) {
        String[] kv = new String[16];
        int n = 0;
        long[] ts = new long[8];
        double[] vs = new double[8];
        int ns = 0;
        while (r.more()) {
            int key = (int) r.varint();
            int field = key >>> 3;
            if (field == 1 && (key & 7) == 2) {
                int len = (int) r.varint();
                Reader l = new Reader(r.b, r.p, r.p + len);
                r.p += len;
                String name = null, value = null;
                while (l.more()) {
                    int k = (int) l.varint();
                    if ((k & 7) == 2 && (k >>> 3 == 1 || k >>> 3 == 2)) {
                        int sl = (int) l.varint();
                        String s = new String(l.b, l.p, sl, StandardCharsets.UTF_8);
                        l.p += sl;
                        if (k >>> 3 == 1) name = s; else value = s;
                    } else {
                        l.skip(k & 7);
                    }
                }
                if (name != null && value != null && !value.isEmpty()) {
                    if (n + 2 > kv.length) kv = Arrays.copyOf(kv, kv.length * 2);
                    kv[n++] = name;
                    kv[n++] = value;
                }
            } else if (field == 2 && (key & 7) == 2) {
                int len = (int) r.varint();
                Reader s = new Reader(r.b, r.p, r.p + len);
                r.p += len;
                double v = 0;
                long t = 0;
                while (s.more()) {
                    int k = (int) s.varint();
                    if (k == (1 << 3 | 1)) v = Double.longBitsToDouble(s.fixed64());
                    else if (k == (2 << 3)) t = s.varint();
                    else s.skip(k & 7);
                }
                if (ns == ts.length) { ts = Arrays.copyOf(ts, ns * 2); vs = Arrays.copyOf(vs, ns * 2); }
                ts[ns] = t;
                vs[ns] = v;
                ns++;
            } else {
                r.skip(key & 7);
            }
        }
        if (ns == 0 || n == 0) return 0;
        // labels are sorted by the sender per spec, but do not rely on it
        java.util.TreeMap<String, String> m = new java.util.TreeMap<>();
        for (int i = 0; i < n; i += 2) m.put(kv[i], kv[i + 1]);
        Labels labels = Labels.fromMap(m);
        for (int i = 0; i < ns; i++) out.add(labels, ts[i], vs[i]);
        return ns;
    }

    private static final class Reader {
        final byte[] b;
        int p;
        final int end;

        Reader(byte[] b, int p, int end) {
            this.b = b;
            this.p = p;
            this.end = end;
        }

        boolean more() { return p < end; }

        long varint() {
            long v = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                if (p >= end) throw new IllegalArgumentException("protobuf: truncated varint");
                byte x = b[p++];
                v |= (long) (x & 0x7f) << shift;
                if (x >= 0) return v;
            }
            throw new IllegalArgumentException("protobuf: varint too long");
        }

        long fixed64() {
            long v = 0;
            for (int k = 0; k < 8; k++) v |= (long) (b[p++] & 0xff) << (8 * k);
            return v;
        }

        void skip(int wire) {
            switch (wire) {
                case 0 -> varint();
                case 1 -> p += 8;
                case 2 -> { int len = (int) varint(); p += len; }
                case 5 -> p += 4;
                default -> throw new IllegalArgumentException("protobuf: unsupported wire type " + wire);
            }
        }
    }
}
