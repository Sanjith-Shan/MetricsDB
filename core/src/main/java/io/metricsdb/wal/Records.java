package io.metricsdb.wal;

import io.metricsdb.model.Labels;
import io.metricsdb.util.ByteIn;
import io.metricsdb.util.ByteOut;

/** WAL record payloads: series definitions and sample batches that reference series by id. */
public final class Records {
    public static final int SERIES = 1;
    public static final int SAMPLES = 2;

    private Records() {}

    public static byte[] series(int id, Labels l) {
        ByteOut o = new ByteOut(64);
        o.u8(SERIES).uvarint(1).uvarint(id).uvarint(l.size());
        for (int i = 0; i < l.size(); i++) o.str(l.name(i)).str(l.value(i));
        return o.toByteArray();
    }

    /** Sample batch: ids, timestamps (delta from the first) and raw IEEE-754 value bits. */
    public static final class SamplesBuilder {
        private final ByteOut o;
        private int count;
        private long base;
        private final int countAt;

        public SamplesBuilder(int expected) {
            o = new ByteOut(16 + expected * 14);
            o.u8(SAMPLES);
            countAt = o.length();
            o.i32(0);
        }

        public void add(int id, long t, double v) {
            if (count == 0) {
                base = t;
                o.varint(base);
            }
            o.uvarint(id).varint(t - base).i64(Double.doubleToRawLongBits(v));
            count++;
        }

        public int count() { return count; }

        public byte[] build() {
            o.setI32(countAt, count);
            return o.toByteArray();
        }
    }

    public interface Visitor {
        void series(int id, Labels labels);
        void sample(int id, long t, double v);
    }

    public static void decode(byte[] payload, Visitor v) {
        ByteIn in = new ByteIn(payload);
        int type = in.u8();
        if (type == SERIES) {
            int n = (int) in.uvarint();
            for (int i = 0; i < n; i++) {
                int id = (int) in.uvarint();
                int nl = (int) in.uvarint();
                String[] kv = new String[nl * 2];
                for (int j = 0; j < kv.length; j++) kv[j] = in.str();
                v.series(id, Labels.fromSorted(kv));
            }
        } else if (type == SAMPLES) {
            int n = in.i32();
            if (n == 0) return;
            long base = in.varint();
            for (int i = 0; i < n; i++) {
                int id = (int) in.uvarint();
                long t = base + in.varint();
                double val = Double.longBitsToDouble(in.i64());
                v.sample(id, t, val);
            }
        } else {
            throw new IllegalStateException("unknown WAL record type " + type);
        }
    }
}
