package com.linkedin.metadata.usage.flush;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.identity.AspectCorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdaptiveFlushCoordinatorTest {

  private UsageOperationsRegistry usageRegistry;
  private UsageMetricRegistry metricRegistry;
  private UsageActorClassResolver actorClassResolver;

  @BeforeMethod
  public void setup() {
    YAMLMapper yamlMapper = yamlMapper();
    usageRegistry = UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper));
    metricRegistry =
        UsageMetricRegistry.loadBundled(new UsageMetricRegistryLoader(yamlMapper), List.of());
    actorClassResolver = new UsageActorClassResolver(new AspectCorpUserFlagsProvider());
  }

  @Test
  public void testZeroIntervalDoesNotScheduleTicks() throws Exception {
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = store(sink, 300);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 0)) {
      Thread.sleep(200);
      Assert.assertTrue(sink.batches().isEmpty());
    }
    Assert.assertEquals(sink.batches().size(), 1);
    Assert.assertEquals(sink.batches().get(0).trigger(), FlushTrigger.SHUTDOWN);
  }

  @Test
  public void testScheduledTickFlushesWhenWindowNotExpired() throws Exception {
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = store(sink, 300);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 1)) {
      Thread.sleep(1500);
      Assert.assertTrue(
          sink.batches().stream().anyMatch(batch -> batch.trigger() == FlushTrigger.SCHEDULED));
    }
  }

  @Test
  public void testScheduledTickFlushesMaxWindowWhenExpired() throws Exception {
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = store(sink, 1);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 1)) {
      Thread.sleep(1500);
      Assert.assertTrue(
          sink.batches().stream().anyMatch(batch -> batch.trigger() == FlushTrigger.MAX_WINDOW));
    }
  }

  @Test
  public void testCloseFlushesOnShutdown() {
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = store(sink, 300);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 0)) {
      // close() triggers shutdown flush
    }
    Assert.assertEquals(sink.batches().size(), 1);
    Assert.assertEquals(sink.batches().get(0).trigger(), FlushTrigger.SHUTDOWN);
  }

  @Test
  public void testTickSurvivesFlushFailure() throws Exception {
    InMemoryUsageAggregationStore store = store(new ThrowingUsageFlushSink(), 300);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 1)) {
      Thread.sleep(1500);
    }
  }

  private InMemoryUsageAggregationStore store(UsageFlushSink sink, long maxWindowSeconds) {
    return new InMemoryUsageAggregationStore(
        usageRegistry, metricRegistry, actorClassResolver, sink, 10_000, maxWindowSeconds);
  }

  private static YAMLMapper yamlMapper() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return yamlMapper;
  }

  private static final class ThrowingUsageFlushSink implements UsageFlushSink {
    @Override
    public void publish(@Nonnull UsageFlushBatch batch) {
      throw new RuntimeException("simulated sink failure");
    }
  }
}
