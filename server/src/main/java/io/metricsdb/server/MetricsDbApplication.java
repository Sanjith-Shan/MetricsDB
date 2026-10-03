package io.metricsdb.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(MetricsDbProperties.class)
@EnableScheduling
public class MetricsDbApplication {
    public static void main(String[] args) {
        SpringApplication.run(MetricsDbApplication.class, args);
    }
}
