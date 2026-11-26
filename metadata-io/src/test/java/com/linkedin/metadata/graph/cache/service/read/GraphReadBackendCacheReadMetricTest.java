package com.linkedin.metadata.graph.cache.service.read;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import org.testng.annotations.Test;

/**
 * Hits and misses share one Prometheus series name. Both must carry the same tag keys, or the
 * second registration throws and the cached expand/walk fails instead of returning its result.
 */
public class GraphReadBackendCacheReadMetricTest {

  private static final EntityGraphDefinition DEFINITION =
      EntityGraphDefinition.builder()
          .graphId("container")
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
          .build();

  @Test
  public void hitAndMissShareTagKeys() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      GraphReadBackend backend = backend(registry);

      backend.recordCachedRead(DEFINITION, "hit", null);
      backend.recordCachedRead(DEFINITION, "stale_hit", null);
      backend.recordCachedRead(DEFINITION, "miss", ReadMissReason.ABSENT);

      assertEquals(count(registry, "hit", "none"), 1.0);
      assertEquals(count(registry, "stale_hit", "none"), 1.0);
      assertEquals(count(registry, "miss", "ABSENT"), 1.0);
    } finally {
      registry.close();
    }
  }

  @Test
  public void missThenHitSharesTagKeys() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      GraphReadBackend backend = backend(registry);

      backend.recordCachedRead(DEFINITION, "miss", ReadMissReason.STALE_BLOCKED);
      backend.recordCachedRead(DEFINITION, "hit", null);

      assertEquals(count(registry, "miss", "STALE_BLOCKED"), 1.0);
      assertEquals(count(registry, "hit", "none"), 1.0);
    } finally {
      registry.close();
    }
  }

  private static GraphReadBackend backend(PrometheusMeterRegistry registry) {
    MetricUtils metricUtils = MetricUtils.builder().registry(registry).build();
    OperationContext operationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .metricUtils(metricUtils)
                        .tracer(OpenTelemetry.noop().getTracer("test"))
                        .build())
            .buildSystemContext();
    return new GraphReadBackend(null, null, null, operationContext, null, null);
  }

  private static double count(PrometheusMeterRegistry registry, String outcome, String reason) {
    Counter counter =
        registry.counter(
            "entity.graph.cache.read",
            "graphId",
            "container",
            "outcome",
            outcome,
            "reason",
            reason);
    return counter.count();
  }
}
