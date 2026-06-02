package com.linkedin.gms.factory.kafka.trace;

import com.linkedin.metadata.config.messaging.KafkaOrPgQueueMessagingTransportCondition;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(KafkaOrPgQueueMessagingTransportCondition.class)
public class TraceReaderThreadPoolConfiguration {

  @Value("${trace.executor.thread-pool-size:10}")
  private int threadPoolSize;

  @Value("${trace.executor.shutdown-timeout-seconds:60}")
  private int shutdownTimeoutSeconds;

  private ExecutorService traceExecutorService;

  @Bean("traceExecutorService")
  public ExecutorService traceExecutorService(MetricUtils metricUtils) {
    traceExecutorService = Executors.newFixedThreadPool(threadPoolSize);
    if (metricUtils != null) {
      MicrometerMetricsRegistry.registerExecutorMetrics(
          "api-trace", traceExecutorService, metricUtils.getRegistry());
    }
    return traceExecutorService;
  }

  @PreDestroy
  public void shutdown() {
    if (traceExecutorService != null) {
      traceExecutorService.shutdown();
      try {
        if (!traceExecutorService.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
          traceExecutorService.shutdownNow();
          if (!traceExecutorService.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
            System.err.println("ExecutorService did not terminate");
          }
        }
      } catch (InterruptedException e) {
        traceExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
