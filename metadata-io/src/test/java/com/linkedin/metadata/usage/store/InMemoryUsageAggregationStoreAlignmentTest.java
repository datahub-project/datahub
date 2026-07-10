package com.linkedin.metadata.usage.store;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.flush.RecordingUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushBatch;
import com.linkedin.metadata.usage.flush.UsageFlushBoundaryUtils;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InMemoryUsageAggregationStoreAlignmentTest {

  private static final ZoneId UTC = ZoneOffset.UTC;

  private UsageOperationsRegistry usageRegistry;
  private UsageMetricRegistry metricRegistry;
  private UsageActorClassResolver actorClassResolver;

  @BeforeMethod
  public void setup() {
    YAMLMapper yamlMapper = yamlMapper();
    usageRegistry = UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper));
    metricRegistry =
        UsageMetricRegistry.loadBundled(new UsageMetricRegistryLoader(yamlMapper), List.of());
    actorClassResolver =
        new UsageActorClassResolver(
            new CorpUserFlagsProvider() {
              @Override
              public boolean isSystemCorpUser(String corpUserUrn) {
                return false;
              }

              @Override
              public boolean isSupportUser(String corpUserUrn) {
                return false;
              }

              @Override
              public CorpUserFlags resolveWithContext(
                  OperationContext opContext, String corpUserUrn) {
                return new CorpUserFlags(false, false);
              }
            });
  }

  @Test
  public void testAlignedWindowStartsOnGrid() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:37:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store =
        alignedStore(sink, clock, Duration.ofHours(1), 10_000, 300);

    Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T10:00:00Z"));
  }

  @Test
  public void testAlignedBatchDoesNotCrossHourBoundary() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:50:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store =
        alignedStore(sink, clock, Duration.ofHours(1), 10_000, 300);

    store.recordRequest(session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    clock.advance(Duration.ofMinutes(9).plusSeconds(30));
    store.flush(FlushTrigger.SCHEDULED);

    Assert.assertEquals(sink.batches().size(), 1);
    UsageFlushBatch batch = sink.batches().get(0);
    Assert.assertFalse(
        UsageFlushBoundaryUtils.crossesBoundary(
            batch.windowStart(), batch.windowEnd(), Duration.ofHours(1), UTC));
    Assert.assertTrue(batch.windowEnd().isBefore(Instant.parse("2026-07-10T11:00:00Z")));
    Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T10:00:00Z"));

    clock.advance(Duration.ofSeconds(30));
    store.flush(FlushTrigger.SCHEDULED);
    Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T11:00:00Z"));
  }

  @Test
  public void testFifteenMinuteAlignment() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:17:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store =
        alignedStore(sink, clock, Duration.ofSeconds(900), 10_000, 300);

    store.recordRequest(session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    clock.advance(Duration.ofMinutes(2));
    store.flush(FlushTrigger.SCHEDULED);

    UsageFlushBatch batch = sink.batches().get(0);
    Assert.assertEquals(batch.windowStart(), Instant.parse("2026-07-10T10:15:00Z"));
    Assert.assertFalse(
        UsageFlushBoundaryUtils.crossesBoundary(
            batch.windowStart(), batch.windowEnd(), Duration.ofSeconds(900), UTC));
  }

  @Test
  public void testCardinalityMidPeriodFlushStaysWithinBucket() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:00:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = alignedStore(sink, clock, Duration.ofHours(1), 2, 300);

    store.recordRequest(session("urn:li:corpuser:a", "metadata_read", null));
    store.recordRequest(session("urn:li:corpuser:b", "metadata_read", null));

    Assert.assertTrue(
        sink.batches().stream().anyMatch(batch -> batch.trigger() == FlushTrigger.CARDINALITY));
    UsageFlushBatch batch =
        sink.batches().stream()
            .filter(b -> b.trigger() == FlushTrigger.CARDINALITY)
            .findFirst()
            .orElseThrow();
    Assert.assertFalse(
        UsageFlushBoundaryUtils.crossesBoundary(
            batch.windowStart(), batch.windowEnd(), Duration.ofHours(1), UTC));
    Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T10:00:00Z"));
  }

  @Test
  public void testFlushPastBoundarySplitsAtBoundary() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:50:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store =
        alignedStore(sink, clock, Duration.ofHours(1), 10_000, 300);

    store.recordRequest(session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    clock.advance(Duration.ofMinutes(15));
    store.flush(FlushTrigger.SCHEDULED);

    Assert.assertEquals(sink.batches().size(), 1);
    UsageFlushBatch batch = sink.batches().get(0);
    Assert.assertEquals(batch.windowStart(), Instant.parse("2026-07-10T10:00:00Z"));
    Assert.assertEquals(batch.windowEnd(), Instant.parse("2026-07-10T11:00:00Z"));
    Assert.assertEquals(store.windowStartSnapshot(), Instant.parse("2026-07-10T11:00:00Z"));
  }

  @Test
  public void testAlignmentDisabledMatchesProcessRelativeWindows() {
    MutableClock clock = new MutableClock(Instant.parse("2026-07-10T10:50:00Z"));
    RecordingUsageFlushSink sink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store =
        new InMemoryUsageAggregationStore(
            usageRegistry,
            metricRegistry,
            actorClassResolver,
            sink,
            10_000,
            300,
            3,
            100L,
            null,
            clock);

    Instant windowStart = store.windowStartSnapshot();
    Assert.assertEquals(windowStart, clock.instant());
    clock.advance(Duration.ofMinutes(5));
    store.flush(FlushTrigger.SCHEDULED);
    UsageFlushBatch batch = sink.batches().get(0);
    Assert.assertEquals(batch.windowStart(), windowStart);
    Assert.assertTrue(batch.windowEnd().isAfter(windowStart));
  }

  private InMemoryUsageAggregationStore alignedStore(
      RecordingUsageFlushSink sink,
      MutableClock clock,
      Duration alignmentPeriod,
      int maxCardinality,
      long maxWindowSeconds) {
    return new InMemoryUsageAggregationStore(
        usageRegistry,
        metricRegistry,
        actorClassResolver,
        sink,
        maxCardinality,
        maxWindowSeconds,
        3,
        100L,
        alignmentPeriod.getSeconds(),
        clock);
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
    private final ZoneId zone;

    MutableClock(@Nonnull Instant start) {
      this.instant = start;
      this.zone = ZoneOffset.UTC;
    }

    void advance(@Nonnull Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
