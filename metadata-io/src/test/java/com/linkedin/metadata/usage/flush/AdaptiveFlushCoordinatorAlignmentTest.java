package com.linkedin.metadata.usage.flush;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.identity.AspectCorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdaptiveFlushCoordinatorAlignmentTest {

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
  public void testHourlyAlignmentFlushesBeforeBoundary() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:50:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = alignedStore(sink, clock, 3600L, 3600);

    AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 30, clock, false);
    try {
      clock.advance(Duration.ofMinutes(9).plusSeconds(30));
      coordinator.tick();

      Assert.assertFalse(sink.batches().isEmpty());
      UsageFlushBatch batch =
          sink.batches().stream()
              .filter(b -> b.trigger() == FlushTrigger.SCHEDULED)
              .findFirst()
              .orElseThrow();
      Assert.assertTrue(batch.windowEnd().isBefore(Instant.parse("2026-07-10T11:00:00Z")));
      Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T10:00:00Z"));

      clock.advance(Duration.ofSeconds(30));
      coordinator.tick();
      Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T11:00:00Z"));
    } finally {
      coordinator.shutdown();
    }
  }

  @Test
  public void testFifteenMinuteAlignmentTickStaysWithinBucket() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:14:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = alignedStore(sink, clock, 900L, 3600);

    AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 30, clock, false);
    try {
      clock.advance(Duration.ofMinutes(1));
      coordinator.tick();
    } finally {
      coordinator.shutdown();
    }

    UsageFlushBatch batch =
        sink.batches().stream()
            .filter(b -> b.trigger() == FlushTrigger.SCHEDULED)
            .findFirst()
            .orElseThrow();
    Assert.assertFalse(
        UsageFlushBoundaryUtils.crossesBoundary(
            batch.windowStart(), batch.windowEnd(), Duration.ofSeconds(900), ZoneOffset.UTC));
  }

  @Test
  public void testRandomFlushSequencesNeverCrossBoundary() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T00:00:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = alignedStore(sink, clock, 3600L, 3600);
    AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 30, clock, false);
    Random random = new Random(42);

    try {
      for (int i = 0; i < 50; i++) {
        store.recordRequest(session("urn:li:corpuser:user-" + i, "metadata_read", null));
        clock.advance(Duration.ofSeconds(10 + random.nextInt(20)));
        coordinator.tick();
      }
    } finally {
      coordinator.shutdown();
    }

    for (UsageFlushBatch batch : sink.batches()) {
      if (batch.trigger() == FlushTrigger.SHUTDOWN) {
        continue;
      }
      Assert.assertFalse(
          UsageFlushBoundaryUtils.crossesBoundary(
              batch.windowStart(), batch.windowEnd(), Duration.ofHours(1), ZoneOffset.UTC),
          "batch crossed boundary: " + batch.windowStart() + " -> " + batch.windowEnd());
    }
  }

  @Test
  public void testAlignmentOffPreservesExistingScheduledBehavior() throws Exception {
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = store(sink, 300);
    try (AdaptiveFlushCoordinator coordinator = new AdaptiveFlushCoordinator(store, 1)) {
      Thread.sleep(1500);
      Assert.assertTrue(
          sink.batches().stream().anyMatch(batch -> batch.trigger() == FlushTrigger.SCHEDULED));
    }
  }

  private InMemoryUsageAggregationStore alignedStore(
      RecordingUsageFlushSink sink,
      MutableClock clock,
      long alignmentPeriodSeconds,
      long maxWindow) {
    return new InMemoryUsageAggregationStore(
        usageRegistry,
        metricRegistry,
        actorClassResolver,
        sink,
        10_000,
        maxWindow,
        3,
        100L,
        alignmentPeriodSeconds,
        clock);
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

  private static OperationContext session(String actorUrn, String usageOperation, Long inputBytes) {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(actorUrn)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("testAction")
            .userAgent("test-agent")
            .usageOperation(usageOperation)
            .usageIdentity(actorUrn)
            .authChannel(AuthChannel.SESSION)
            .inputBytes(inputBytes)
            .requestBodyMaterialized(inputBytes != null)
            .build();
    try {
      return TestOperationContexts.systemContextNoValidate().toBuilder()
          .requestContext(requestContext)
          .build(userAuth(actorUrn), true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Authentication userAuth(String actorUrn) {
    String actorId =
        actorUrn.startsWith("urn:li:corpuser:")
            ? actorUrn.substring("urn:li:corpuser:".length())
            : actorUrn;
    return new Authentication(new Actor(ActorType.USER, actorId), "Basic test");
  }

  static final class MutableClock extends Clock {
    private Instant instant;

    MutableClock(@Nonnull Instant start) {
      this.instant = start;
    }

    void advance(@Nonnull Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public java.time.ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
