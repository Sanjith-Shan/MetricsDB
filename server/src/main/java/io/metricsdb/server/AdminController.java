package io.metricsdb.server;

import io.metricsdb.block.Block;
import io.metricsdb.server.cluster.AntiEntropy;
import io.metricsdb.storage.Tsdb;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operational endpoints: flush and maintenance, stats, cluster status and anti-entropy repair. */
@RestController
public class AdminController {
    private final NodeConfig.Node node;

    public AdminController(NodeConfig.Node node) {
        this.node = node;
    }

    @PostMapping(value = "/admin/flush", produces = MediaType.APPLICATION_JSON_VALUE)
    public String flush() {
        if (node.tsdb() == null) return Json.error("bad_data", "flush is per storage node");
        long t0 = System.nanoTime();
        node.tsdb().flushHead();
        return "{\"status\":\"success\",\"millis\":" + (System.nanoTime() - t0) / 1_000_000 + "}";
    }

    @PostMapping(value = "/admin/maintain", produces = MediaType.APPLICATION_JSON_VALUE)
    public String maintain() {
        if (node.tsdb() == null) return Json.error("bad_data", "maintenance is per storage node");
        node.tsdb().maintain();
        return "{\"status\":\"success\"}";
    }

    @GetMapping(value = "/admin/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public String stats() {
        if (node.tsdb() == null) {
            StringBuilder sb = new StringBuilder("{\"role\":\"router\",\"nodes\":[");
            var c = node.cluster();
            for (int i = 0; i < c.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append("{\"name\":");
                Json.str(sb, c.nodes.get(i).name);
                sb.append(",\"up\":").append(c.isUp(i)).append(",\"hintBytes\":").append(node.hints() == null ? 0 : node.hints().pendingBytes(i)).append('}');
            }
            sb.append("],\"hintsWritten\":").append(node.hints() == null ? 0 : node.hints().written.get())
                    .append(",\"hintsReplayed\":").append(node.hints() == null ? 0 : node.hints().replayed.get());
            return sb.append('}').toString();
        }
        Tsdb db = node.tsdb();
        Tsdb.Stats s = db.stats();
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"role\":\"").append(node.props().getRole()).append("\",");
        sb.append("\"node\":");
        Json.str(sb, node.props().getNodeName());
        sb.append(",\"series\":").append(s.series())
                .append(",\"headSamples\":").append(s.headSamples())
                .append(",\"headChunkBytes\":").append(s.headChunkBytes())
                .append(",\"blocks\":").append(s.blocks())
                .append(",\"blockSamples\":").append(s.blockSamples())
                .append(",\"blockChunkBytes\":").append(s.blockChunkBytes())
                .append(",\"blockRollupBytes\":").append(s.blockRollupBytes())
                .append(",\"blockBytesOnDisk\":").append(s.blockBytesOnDisk())
                .append(",\"walBytesOnDisk\":").append(s.walBytesOnDisk())
                .append(",\"samplesAppended\":").append(db.samplesAppended.get())
                .append(",\"samplesOutOfOrder\":").append(db.samplesOutOfOrder.get())
                .append(",\"samplesDuplicate\":").append(db.samplesDuplicate.get())
                .append(",\"samplesTooOld\":").append(db.samplesTooOld.get())
                .append(",\"samplesOverCardinality\":").append(db.samplesOverCardinality.get())
                .append(",\"walFsyncs\":").append(db.wal().fsyncCount())
                .append(",\"replayMillis\":").append(db.replayMillis())
                .append(",\"replayRecords\":").append(db.replayStats().records())
                .append(",\"replayTornTails\":").append(db.replayStats().tornTails())
                .append(",\"blockList\":[");
        boolean first = true;
        for (Block b : db.blocks()) {
            if (!first) sb.append(',');
            first = false;
            sb.append("{\"dir\":");
            Json.str(sb, b.dir.getFileName().toString());
            sb.append(",\"level\":").append(b.meta.level()).append(",\"series\":").append(b.meta.series())
                    .append(",\"samples\":").append(b.meta.samples()).append(",\"bytes\":").append(b.bytesOnDisk()).append('}');
        }
        return sb.append("]}").toString();
    }

    @PostMapping(value = "/admin/repair", produces = MediaType.APPLICATION_JSON_VALUE)
    public String repair(@RequestParam(defaultValue = "0") long mint, @RequestParam(defaultValue = "0") long maxt,
                         @RequestParam(defaultValue = "3600000") long bucketMs) {
        AntiEntropy ae = node.antiEntropy();
        if (ae == null) return Json.error("bad_data", "repair runs on the router");
        AntiEntropy.Report r = ae.run(mint == 0 ? Long.MIN_VALUE : mint, maxt == 0 ? Long.MAX_VALUE : maxt, bucketMs);
        return "{\"ranges\":" + r.ranges() + ",\"rangesMismatched\":" + r.rangesMismatched() + ",\"seriesCompared\":" + r.seriesCompared()
                + ",\"seriesRepaired\":" + r.seriesRepaired() + ",\"samplesWritten\":" + r.samplesWritten() + ",\"millis\":" + r.millis()
                + ",\"complete\":" + r.complete() + "}";
    }
}
