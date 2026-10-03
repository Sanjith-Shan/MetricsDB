package io.metricsdb.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/** Machine description and load, attached to every result row, and the results-file writer. */
public final class Env {
    public static final ObjectMapper JSON = new ObjectMapper();

    private Env() {}

    public static ObjectNode machine() {
        ObjectNode m = JSON.createObjectNode();
        m.put("host", "mini PC (Acemagic K1)");
        m.put("cpu", cpuModel());
        m.put("cores_host", 4);
        m.put("cpus_visible", Runtime.getRuntime().availableProcessors());
        m.put("ram_gb_host", 16);
        m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " (WSL2)");
        m.put("java", System.getProperty("java.vm.version"));
        m.put("git", System.getenv().getOrDefault("GIT_REV", "unknown"));
        m.put("note", "the database under test, the load generator and the query client share the WSL2 VM (2 vCPU, 6 GB)");
        return m;
    }

    static String cpuModel() {
        try {
            for (String l : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                if (l.startsWith("model name")) return l.substring(l.indexOf(':') + 1).trim();
            }
        } catch (IOException ignored) {
        }
        return "unknown";
    }

    /** 1-minute load average inside WSL plus the Windows host CPU percentage when the caller passes it in. */
    public static ObjectNode load() {
        ObjectNode o = JSON.createObjectNode();
        try {
            String[] f = Files.readString(Path.of("/proc/loadavg")).split(" ");
            o.put("wsl_load1", Double.parseDouble(f[0]));
            o.put("wsl_load5", Double.parseDouble(f[1]));
        } catch (IOException ignored) {
        }
        String host = System.getenv("HOST_CPU_PCT");
        if (host != null) o.put("windows_host_cpu_pct", Double.parseDouble(host));
        return o;
    }

    public static String now() {
        return OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    public static ObjectNode hist(Histogram h, double scale) {
        ObjectNode o = JSON.createObjectNode();
        o.put("count", h.getTotalCount());
        if (h.getTotalCount() == 0) return o;
        o.put("p50", h.getValueAtPercentile(50) / scale);
        o.put("p90", h.getValueAtPercentile(90) / scale);
        o.put("p99", h.getValueAtPercentile(99) / scale);
        o.put("p999", h.getValueAtPercentile(99.9) / scale);
        o.put("max", h.getMaxValue() / scale);
        o.put("mean", Math.round(h.getMean() / scale * 1000) / 1000.0);
        return o;
    }

    public static void append(String file, ObjectNode row) throws IOException {
        String line = JSON.writeValueAsString(row) + "\n";
        System.out.print(line);
        if (file != null) {
            Path p = Path.of(file);
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            Files.writeString(p, line, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }
}
