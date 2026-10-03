package io.metricsdb.ingest;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IngestFormatsTest {

    @Test
    void lineProtocolMapsFieldsToSeriesLikeVictoriaMetrics() {
        String body = "cpu,hostname=host_0,region=eu-west-1 usage_user=58i,usage_system=2.5 1790812800000000000\n"
                + "# comment\n"
                + "mem,hostname=host_0 used_percent=12.25,flag=true,name=\"str\" 1790812810000000000\n";
        WriteBatch b = new WriteBatch();
        LineProtocol.Stats st = new LineProtocol().parse(body.getBytes(StandardCharsets.UTF_8), body.length(), 1_000_000, b);
        assertEquals(0, st.errors);
        assertEquals(4, b.n);
        assertEquals(Labels.of("__name__", "cpu_usage_user", "hostname", "host_0", "region", "eu-west-1"), b.labels[0]);
        assertEquals(58.0, b.v[0]);
        assertEquals(1790812800000L, b.t[0]);
        assertEquals(2.5, b.v[1]);
        assertEquals(Labels.of("__name__", "mem_used_percent", "hostname", "host_0"), b.labels[2]);
        assertEquals(1.0, b.v[3]);
        assertEquals(1, st.skippedFields);
    }

    @Test
    void lineProtocolHandlesEscapesAndBadLines() {
        String body = "my\\ metric,tag\\,x=a\\ b value=-1.5e3 1000\nbroken line\n";
        WriteBatch b = new WriteBatch();
        LineProtocol.Stats st = new LineProtocol().parse(body.getBytes(StandardCharsets.UTF_8), body.length(), -1000, b);
        assertEquals(1, st.errors);
        assertEquals(1, b.n);
        assertEquals("my metric_value", b.labels[0].metricName());
        assertEquals("a b", b.labels[0].get("tag,x"));
        assertEquals(-1500.0, b.v[0]);
        assertEquals(1_000_000L, b.t[0]);
    }

    @Property(tries = 300)
    void decimalFastPathMatchesDoubleParse(@ForAll double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) return;
        String s = Double.toString(d).replace("E", "e");
        String body = "m v=" + s + " 1\n";
        WriteBatch b = new WriteBatch();
        new LineProtocol().parse(body.getBytes(StandardCharsets.US_ASCII), body.length(), 1, b);
        assertEquals(Double.doubleToRawLongBits(Double.parseDouble(s)), Double.doubleToRawLongBits(b.v[0]), s);
    }

    @Property(tries = 200)
    void snappyDecodesWhatTheReferenceEncoderWrites(@ForAll @Size(max = 5000) byte[] raw) throws IOException {
        byte[] repeated = new byte[raw.length * 3];
        for (int i = 0; i < repeated.length; i++) repeated[i] = raw.length == 0 ? 0 : raw[i % raw.length];
        assertArrayEquals(raw, io.metricsdb.ingest.Snappy.decompress(Snappy.compress(raw)));
        assertArrayEquals(repeated, io.metricsdb.ingest.Snappy.decompress(Snappy.compress(repeated)));
    }

    /** Minimal protobuf writer for a WriteRequest, independent of the decoder under test. */
    static byte[] writeRequest(String[][] series, long[] ts, double[] vs) throws IOException {
        ByteArrayOutputStream req = new ByteArrayOutputStream();
        for (String[] labels : series) {
            ByteArrayOutputStream tsb = new ByteArrayOutputStream();
            for (int i = 0; i < labels.length; i += 2) {
                ByteArrayOutputStream l = new ByteArrayOutputStream();
                field(l, 1, labels[i].getBytes(StandardCharsets.UTF_8));
                field(l, 2, labels[i + 1].getBytes(StandardCharsets.UTF_8));
                field(tsb, 1, l.toByteArray());
            }
            for (int i = 0; i < ts.length; i++) {
                ByteArrayOutputStream s = new ByteArrayOutputStream();
                s.write(1 << 3 | 1);
                long bits = Double.doubleToRawLongBits(vs[i]);
                for (int k = 0; k < 8; k++) s.write((int) (bits >>> (8 * k)));
                s.write(2 << 3);
                varint(s, ts[i]);
                field(tsb, 2, s.toByteArray());
            }
            field(req, 1, tsb.toByteArray());
        }
        return req.toByteArray();
    }

    static void field(ByteArrayOutputStream o, int num, byte[] b) {
        o.write(num << 3 | 2);
        varint(o, b.length);
        o.writeBytes(b);
    }

    static void varint(ByteArrayOutputStream o, long v) {
        while ((v & ~0x7FL) != 0) { o.write((int) ((v & 0x7F) | 0x80)); v >>>= 7; }
        o.write((int) v);
    }

    @Test
    void remoteWriteDecodesSeriesAndSamples() throws IOException {
        byte[] pb = writeRequest(new String[][]{{"__name__", "up", "job", "node", "instance", "a:9100"}, {"job", "x", "__name__", "y"}},
                new long[]{1_700_000_000_000L, 1_700_000_015_000L}, new double[]{1, Double.NaN});
        WriteBatch b = new WriteBatch();
        int n = RemoteWrite.decode(Snappy.compress(pb), b);
        assertEquals(4, n);
        assertEquals(Labels.of("__name__", "up", "instance", "a:9100", "job", "node"), b.labels[0]);
        assertEquals(Labels.of("__name__", "y", "job", "x"), b.labels[2]);
        assertEquals(1_700_000_015_000L, b.t[1]);
        assertEquals(Double.doubleToRawLongBits(Double.NaN), Double.doubleToRawLongBits(b.v[1]));
    }
}
