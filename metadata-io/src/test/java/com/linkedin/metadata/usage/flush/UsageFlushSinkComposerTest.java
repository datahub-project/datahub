package com.linkedin.metadata.usage.flush;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageFlushSinkComposerTest {

  private static UsageMetricRegistry metricRegistry() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return UsageMetricRegistry.loadBundled(
        new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
  }

  @Test
  public void testDelegatesToAllSinks() {
    AtomicInteger calls = new AtomicInteger();
    UsageFlushSink sinkA = (opContext, batch) -> calls.incrementAndGet();
    UsageFlushSink sinkB = (opContext, batch) -> calls.addAndGet(10);
    UsageFlushSinkComposer composer = new UsageFlushSinkComposer(List.of(sinkA, sinkB));

    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of());
    composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch);

    Assert.assertEquals(calls.get(), 11);
  }

  @Test
  public void testPropagatesDelegateFailure() {
    UsageFlushSink failingSink =
        (opContext, batch) -> {
          throw new IllegalStateException("sink down");
        };
    UsageFlushSinkComposer composer = new UsageFlushSinkComposer(List.of(failingSink));
    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of());

    Assert.assertThrows(
        IllegalStateException.class,
        () -> composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch));
  }

  @Test
  public void testSkipsSuccessfulDelegatesOnRetry() {
    AtomicInteger firstSinkCalls = new AtomicInteger();
    AtomicInteger secondSinkCalls = new AtomicInteger();
    AtomicInteger secondSinkAttempts = new AtomicInteger();

    UsageFlushSink firstSink = (opContext, batch) -> firstSinkCalls.incrementAndGet();
    UsageFlushSink secondSink =
        (opContext, batch) -> {
          secondSinkCalls.incrementAndGet();
          if (secondSinkAttempts.incrementAndGet() == 1) {
            throw new IllegalStateException("second sink down");
          }
        };
    UsageFlushSinkComposer composer = new UsageFlushSinkComposer(List.of(firstSink, secondSink));
    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of());

    Assert.assertThrows(
        IllegalStateException.class,
        () -> composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch));
    Assert.assertEquals(firstSinkCalls.get(), 1);
    Assert.assertEquals(secondSinkCalls.get(), 1);

    composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch);

    Assert.assertEquals(firstSinkCalls.get(), 1);
    Assert.assertEquals(secondSinkCalls.get(), 2);
  }

  @Test
  public void testPartialFailureRetryDoesNotDoubleMicrometerCounters() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink micrometerSink =
        new MicrometerUsageFlushSink(metricRegistry(), registry);
    AtomicInteger secondaryAttempts = new AtomicInteger();
    UsageFlushSink secondarySink =
        (opContext, batch) -> {
          if (secondaryAttempts.incrementAndGet() == 1) {
            throw new IllegalStateException("secondary sink down");
          }
        };
    UsageFlushSinkComposer composer =
        new UsageFlushSinkComposer(List.of(micrometerSink, secondarySink));

    Map<String, String> dimensions =
        Map.of(
            "usage_operation", "metadata_read", "agent_class", "browser", "request_api", "openapi");
    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(new AdditiveUsageRow("input_bytes", UsageActorClass.REGULAR, dimensions, 100)),
            List.of());

    Assert.assertThrows(
        IllegalStateException.class,
        () -> composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch));
    Assert.assertEquals(registry.get("datahub.usage.input_bytes").counter().count(), 100.0);

    composer.publish(TestOperationContexts.systemContextNoSearchAuthorization(), batch);

    Assert.assertEquals(registry.get("datahub.usage.input_bytes").counter().count(), 100.0);
  }
}
