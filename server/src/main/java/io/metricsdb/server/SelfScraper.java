package io.metricsdb.server;

import io.metricsdb.ingest.Exposition;
import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Dogfooding without Prometheus: when {@code metricsdb.self-scrape.interval} is set, the node
 * renders its own /metrics page and writes it into itself (or through the router), labelled
 * {@code job="metricsdb", instance=<node name>}.
 */
@Component
public class SelfScraper {
    public SelfScraper(MetricsDbProperties props, NodeConfig.Node node, ObjectProvider<PrometheusMeterRegistry> registries,
                       @Value("${server.port:9201}") int port) {
        Duration every = props.getSelfScrape().getInterval();
        PrometheusMeterRegistry registry = registries.getIfAvailable();
        if (every.isZero() || registry == null) return;
        Labels extra = Labels.of("job", "metricsdb", "instance", props.getNodeName());
        Thread.ofVirtual().name("self-scrape").start(() -> {
            while (true) {
                try {
                    Thread.sleep(every.toMillis());
                    WriteBatch b = new WriteBatch(2048);
                    Exposition.parse(registry.scrape(), System.currentTimeMillis(), extra, b);
                    node.ingester().write(b);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception ignored) {
                    // a failed self-scrape must never affect the node
                }
            }
        });
    }
}
