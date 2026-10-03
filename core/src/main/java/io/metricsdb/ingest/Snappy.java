package io.metricsdb.ingest;

/** Snappy block-format decompressor (the framing Prometheus remote write uses), written from the format description. */
public final class Snappy {
    private Snappy() {}

    public static byte[] decompress(byte[] in) {
        return decompress(in, 0, in.length);
    }

    public static byte[] decompress(byte[] in, int off, int len) {
        int end = off + len;
        int p = off;
        long n = 0;
        for (int shift = 0; ; shift += 7) {
            if (p >= end || shift > 35) throw new IllegalArgumentException("snappy: bad length preamble");
            int b = in[p++] & 0xff;
            n |= (long) (b & 0x7f) << shift;
            if (b < 0x80) break;
        }
        if (n > (512L << 20)) throw new IllegalArgumentException("snappy: uncompressed size " + n + " too large");
        byte[] out = new byte[(int) n];
        int o = 0;
        while (p < end) {
            int tag = in[p++] & 0xff;
            switch (tag & 3) {
                case 0 -> {
                    int l = tag >>> 2;
                    if (l >= 60) {
                        int bytes = l - 59;
                        l = 0;
                        for (int k = 0; k < bytes; k++) l |= (in[p++] & 0xff) << (8 * k);
                    }
                    l += 1;
                    if (p + l > end || o + l > out.length) throw new IllegalArgumentException("snappy: literal overruns");
                    System.arraycopy(in, p, out, o, l);
                    p += l;
                    o += l;
                }
                case 1 -> {
                    int l = ((tag >>> 2) & 7) + 4;
                    int offset = ((tag >>> 5) << 8) | (in[p++] & 0xff);
                    o = copy(out, o, offset, l);
                }
                case 2 -> {
                    int l = (tag >>> 2) + 1;
                    int offset = (in[p] & 0xff) | (in[p + 1] & 0xff) << 8;
                    p += 2;
                    o = copy(out, o, offset, l);
                }
                default -> {
                    int l = (tag >>> 2) + 1;
                    int offset = (in[p] & 0xff) | (in[p + 1] & 0xff) << 8 | (in[p + 2] & 0xff) << 16 | (in[p + 3] & 0xff) << 24;
                    p += 4;
                    o = copy(out, o, offset, l);
                }
            }
        }
        if (o != out.length) throw new IllegalArgumentException("snappy: decoded " + o + " of " + out.length + " bytes");
        return out;
    }

    private static int copy(byte[] out, int o, int offset, int l) {
        if (offset <= 0 || offset > o || o + l > out.length) throw new IllegalArgumentException("snappy: bad copy");
        // byte by byte: a copy may overlap its own output (run-length style)
        for (int k = 0; k < l; k++) out[o + k] = out[o - offset + k];
        return o + l;
    }
}
