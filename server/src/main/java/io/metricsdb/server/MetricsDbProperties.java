package io.metricsdb.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Everything configurable, under {@code metricsdb.*}. */
@ConfigurationProperties(prefix = "metricsdb")
public class MetricsDbProperties {
    /** single: storage and query in one process. storage: a storage node. router: writes and queries for a cluster. */
    private String role = "single";
    private String nodeName = "node-1";
    private String dataDir = "./data";

    private final Storage storage = new Storage();
    private final Query query = new Query();
    private final Cluster cluster = new Cluster();
    private final SelfScrape selfScrape = new SelfScrape();

    public static class Storage {
        private Duration blockRange = Duration.ofHours(2);
        private Duration outOfOrderWindow = Duration.ofMinutes(10);
        /** Cardinality limit: most series this node will hold. */
        private int maxSeries = 2_000_000;
        /** always: fsync before acknowledging (group commit). none: leave it to the OS. */
        private String walSync = "always";
        private Duration retentionRaw = Duration.ZERO;
        private Duration retentionRollup = Duration.ZERO;
        private Duration maintenanceInterval = Duration.ofSeconds(5);

        public Duration getBlockRange() { return blockRange; }
        public void setBlockRange(Duration v) { blockRange = v; }
        public Duration getOutOfOrderWindow() { return outOfOrderWindow; }
        public void setOutOfOrderWindow(Duration v) { outOfOrderWindow = v; }
        public int getMaxSeries() { return maxSeries; }
        public void setMaxSeries(int v) { maxSeries = v; }
        public String getWalSync() { return walSync; }
        public void setWalSync(String v) { walSync = v; }
        public Duration getRetentionRaw() { return retentionRaw; }
        public void setRetentionRaw(Duration v) { retentionRaw = v; }
        public Duration getRetentionRollup() { return retentionRollup; }
        public void setRetentionRollup(Duration v) { retentionRollup = v; }
        public Duration getMaintenanceInterval() { return maintenanceInterval; }
        public void setMaintenanceInterval(Duration v) { maintenanceInterval = v; }
    }

    public static class Query {
        private long maxSeries = 100_000;
        private long maxSamples = 200_000_000;
        private Duration timeout = Duration.ofSeconds(30);
        /** off, auto (long steps use rollups) or force. */
        private String rollups = "auto";
        private boolean alignLikeVictoriaMetrics = true;

        public long getMaxSeries() { return maxSeries; }
        public void setMaxSeries(long v) { maxSeries = v; }
        public long getMaxSamples() { return maxSamples; }
        public void setMaxSamples(long v) { maxSamples = v; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration v) { timeout = v; }
        public String getRollups() { return rollups; }
        public void setRollups(String v) { rollups = v; }
        public boolean isAlignLikeVictoriaMetrics() { return alignLikeVictoriaMetrics; }
        public void setAlignLikeVictoriaMetrics(boolean v) { alignLikeVictoriaMetrics = v; }
    }

    public static class Cluster {
        /** Storage nodes as name=url, for the router. */
        private List<String> nodes = new ArrayList<>();
        private int replicationFactor = 2;
        private int vnodes = 64;
        /** Replicas that must be durable before a write is acknowledged; the rest get hints. */
        private int writeQuorum = 1;
        private boolean hintedHandoff = true;
        private boolean readRepair = true;
        private Duration antiEntropyInterval = Duration.ZERO;
        private Duration requestTimeout = Duration.ofSeconds(10);
        private Duration healthInterval = Duration.ofMillis(500);

        public List<String> getNodes() { return nodes; }
        public void setNodes(List<String> v) { nodes = v; }
        public int getReplicationFactor() { return replicationFactor; }
        public void setReplicationFactor(int v) { replicationFactor = v; }
        public int getVnodes() { return vnodes; }
        public void setVnodes(int v) { vnodes = v; }
        public int getWriteQuorum() { return writeQuorum; }
        public void setWriteQuorum(int v) { writeQuorum = v; }
        public boolean isHintedHandoff() { return hintedHandoff; }
        public void setHintedHandoff(boolean v) { hintedHandoff = v; }
        public boolean isReadRepair() { return readRepair; }
        public void setReadRepair(boolean v) { readRepair = v; }
        public Duration getAntiEntropyInterval() { return antiEntropyInterval; }
        public void setAntiEntropyInterval(Duration v) { antiEntropyInterval = v; }
        public Duration getRequestTimeout() { return requestTimeout; }
        public void setRequestTimeout(Duration v) { requestTimeout = v; }
        public Duration getHealthInterval() { return healthInterval; }
        public void setHealthInterval(Duration v) { healthInterval = v; }
    }

    public static class SelfScrape {
        private Duration interval = Duration.ZERO;

        public Duration getInterval() { return interval; }
        public void setInterval(Duration v) { interval = v; }
    }

    public String getRole() { return role; }
    public void setRole(String v) { role = v; }
    public String getNodeName() { return nodeName; }
    public void setNodeName(String v) { nodeName = v; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String v) { dataDir = v; }
    public Storage getStorage() { return storage; }
    public Query getQuery() { return query; }
    public Cluster getCluster() { return cluster; }
    public SelfScrape getSelfScrape() { return selfScrape; }

    public boolean hasLocalStorage() { return role.equals("single") || role.equals("storage"); }
    public boolean isRouter() { return role.equals("router"); }
}
