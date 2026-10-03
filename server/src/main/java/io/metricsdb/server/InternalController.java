package io.metricsdb.server;

import io.metricsdb.cluster.Digests;
import io.metricsdb.cluster.Wire;
import io.metricsdb.index.Matcher;
import io.metricsdb.storage.QueryContext;
import io.metricsdb.storage.RollupSeries;
import io.metricsdb.storage.Tsdb;
import io.metricsdb.storage.WriteBatch;
import io.metricsdb.util.ByteIn;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Storage-node API used by the router: replicated writes, selections as Gorilla chunks, digests. */
@RestController
public class InternalController {
    private final NodeConfig.Node node;
    private final NodeMetrics metrics;

    public InternalController(NodeConfig.Node node, NodeMetrics metrics) {
        this.node = node;
        this.metrics = metrics;
    }

    private Tsdb db() {
        if (node.tsdb() == null) throw new IllegalStateException("this node has no local storage (role " + node.props().getRole() + ")");
        return node.tsdb();
    }

    @GetMapping("/internal/health")
    public String health() { return "ok"; }

    @PostMapping("/internal/write")
    public ResponseEntity<String> write(@RequestBody byte[] body) {
        long t0 = System.nanoTime();
        WriteBatch b = Wire.decodeBatch(body);
        Tsdb.AppendResult r = db().append(b);
        metrics.recordNanos("metricsdb.ingest.request", System.nanoTime() - t0, "protocol", "internal");
        return ResponseEntity.ok((r.accepted() + r.duplicates()) + "," + r.overCardinality() + "," + r.tooOld());
    }

    private record Sel(long mint, long maxt, long maxSeries, List<Matcher> matchers) {}

    private static Sel sel(ByteIn in) {
        long mint = in.varint(), maxt = in.varint(), maxSeries = in.uvarint();
        return new Sel(mint, maxt, maxSeries, Wire.decodeMatchers(in.str()));
    }

    @PostMapping("/internal/select")
    public byte[] select(@RequestBody byte[] body) {
        Sel s = sel(new ByteIn(body));
        QueryContext c = new QueryContext(s.maxSeries, 0, 0);
        return Wire.encodeSeries(db().select(s.matchers, s.mint, s.maxt, c));
    }

    @PostMapping("/internal/select_rollup")
    public byte[] selectRollup(@RequestBody byte[] body) {
        ByteIn in = new ByteIn(body);
        long res = in.varint();
        Sel s = sel(in);
        QueryContext c = new QueryContext(s.maxSeries, 0, 0);
        List<RollupSeries> r = db().selectRollup(s.matchers, s.mint, s.maxt, res, c);
        return r == null ? new byte[0] : Wire.encodeRollups(r);
    }

    @PostMapping("/internal/digest")
    public byte[] digest(@RequestBody byte[] body) {
        return Digests.compute(db(), Digests.Request.decode(body));
    }

    @ExceptionHandler(QueryContext.LimitExceeded.class)
    public ResponseEntity<String> limit(QueryContext.LimitExceeded e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(e.getMessage());
    }
}
