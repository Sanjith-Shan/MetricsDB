package io.metricsdb.server;

import io.metricsdb.promql.Engine;
import io.metricsdb.server.cluster.AntiEntropy;
import io.metricsdb.server.cluster.ClusterState;
import io.metricsdb.server.cluster.FanoutQueryable;
import io.metricsdb.server.cluster.HintStore;
import io.metricsdb.server.cluster.ReplicatingIngester;
import io.metricsdb.storage.Queryable;
import io.metricsdb.storage.Tsdb;
import io.metricsdb.wal.Wal;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Builds the node for its role. single and storage nodes own a {@link Tsdb}; a router owns the
 * cluster view, hint log and anti-entropy, and queries through {@link FanoutQueryable}.
 */
@Configuration
public class NodeConfig {

    /** Holder so beans that only exist for some roles can be injected everywhere. */
    public record Node(MetricsDbProperties props, Tsdb tsdb, ClusterState cluster, HintStore hints,
                       AntiEntropy antiEntropy, Ingester ingester, Queryable queryable, Engine engine) implements AutoCloseable {
        @Override public void close() {
            if (tsdb != null) tsdb.close();
            if (cluster != null) cluster.close();
        }
    }

    @Bean(destroyMethod = "close")
    public Node node(MetricsDbProperties p, NodeMetrics metrics) {
        Path data = Path.of(p.getDataDir());
        Engine.Options eo = new Engine.Options();
        eo.alignLikeVictoriaMetrics = p.getQuery().isAlignLikeVictoriaMetrics();
        if (p.isRouter()) {
            ClusterState cluster = new ClusterState(p.getCluster(), metrics);
            HintStore hints = p.getCluster().isHintedHandoff() ? new HintStore(data.resolve("hints"), cluster.size()) : null;
            if (hints != null) {
                for (int i = 0; i < cluster.size(); i++) {
                    final int n = i;
                    metrics.gauge("metricsdb.router.hints.pending.bytes", () -> hints.pendingBytes(n), "node", cluster.nodes.get(i).name);
                }
            }
            Ingester ing = new ReplicatingIngester(cluster, hints, p.getCluster().getWriteQuorum(), metrics);
            Queryable q = new FanoutQueryable(cluster, metrics, p.getCluster().isReadRepair());
            AntiEntropy ae = new AntiEntropy(cluster, metrics);
            return new Node(p, null, cluster, hints, ae, ing, q, new Engine(q, eo));
        }
        Tsdb.Options o = new Tsdb.Options(data);
        MetricsDbProperties.Storage s = p.getStorage();
        o.blockRangeMs = s.getBlockRange().toMillis();
        o.oooWindowMs = s.getOutOfOrderWindow().toMillis();
        o.maxSeries = s.getMaxSeries();
        o.walSync = s.getWalSync().equalsIgnoreCase("none") ? Wal.Sync.NONE : Wal.Sync.ALWAYS;
        o.retentionRawMs = s.getRetentionRaw().toMillis();
        o.retentionRollupMs = s.getRetentionRollup().toMillis();
        o.maintenanceIntervalMs = s.getMaintenanceInterval().toMillis();
        Tsdb db = new Tsdb(o);
        metrics.bind(db);
        return new Node(p, db, null, null, null, new LocalIngester(db, p.getNodeName()), db, new Engine(db, eo));
    }
}
