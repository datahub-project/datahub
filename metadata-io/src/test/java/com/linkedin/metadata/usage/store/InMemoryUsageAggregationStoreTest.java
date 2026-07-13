package com.linkedin.metadata.usage.store;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.flush.AdditiveUsageRow;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.flush.MicrometerUsageFlushSink;
import com.linkedin.metadata.usage.flush.RecordingUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushBatch;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider.CorpUserFlags;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.instrumentation.UsageRequestState;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InMemoryUsageAggregationStoreTest {

  private static final String SUPPORT_TEST_ACTOR = "urn:li:corpuser:support-user";

  private RecordingUsageFlushSink sink;
  private InMemoryUsageAggregationStore store;

  @Test
  public void testApproximateCardinalityTriggersFlush() {
    sink = new RecordingUsageFlushSink();
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    UsageActorClassResolver actorClassResolver = deterministicActorClassResolver();
    InMemoryUsageAggregationStore lowCardinalityStore =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, actorClassResolver, sink, 5, 300);

    lowCardinalityStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));

    Assert.assertTrue(
        sink.batches().stream().anyMatch(batch -> batch.trigger() == FlushTrigger.CARDINALITY));
    Assert.assertEquals(lowCardinalityStore.currentCardinality(), 0);
  }

  private static YAMLMapper yamlMapper() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return yamlMapper;
  }

  @BeforeMethod
  public void setup() {
    UsageRequestState.clear();
    YAMLMapper yamlMapper = yamlMapper();
    sink = new RecordingUsageFlushSink();
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
    store =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, deterministicActorClassResolver(), sink, 10_000, 300);
  }

  /**
   * Flag-based classification with deterministic corp-user flags for tests. Avoids ThreadLocal
   * corp-user flag cache pollution from other usage tests that run on the same TestNG worker
   * thread.
   */
  private static UsageActorClassResolver deterministicActorClassResolver() {
    return new UsageActorClassResolver(
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
          public CorpUserFlags resolveWithContext(OperationContext opContext, String corpUserUrn) {
            if (SUPPORT_TEST_ACTOR.equals(corpUserUrn)) {
              return new CorpUserFlags(false, true);
            }
            return new CorpUserFlags(false, false);
          }
        });
  }

  @AfterMethod
  public void tearDown() {
    UsageRequestState.clear();
  }

  @Test
  public void testSameIdentityAcrossAuthChannelsCountsOncePerActorClass() {
    store.recordRequest(
        session(
            UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null, AuthChannel.SESSION));
    store.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null, AuthChannel.OAUTH));
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertEquals(distinctIdentityCount(batch, "active_users", "regular"), 1L);
    Assert.assertEquals(distinctIdentityCount(batch, "active_users", "support"), 0L);
    Assert.assertEquals(
        batch.distinctSnapshots().stream()
            .filter(snapshot -> snapshot.metricName().equals("active_users"))
            .count(),
        3L);
    Assert.assertEquals(
        batch.additiveRows().stream()
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum(),
        2L);

    SimpleMeterRegistry micrometerRegistry = new SimpleMeterRegistry();
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    new MicrometerUsageFlushSink(metricRegistry, micrometerRegistry).publish(batch);
    Assert.assertEquals(
        micrometerRegistry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "regular")
            .tag("identity_metric", "active_users")
            .gauge()
            .value(),
        1.0);
  }

  @Test
  public void testRegularWriteEmitsAdditiveAndDistinctRows() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_write", 100L);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    Assert.assertFalse(sink.batches().isEmpty());
    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("api_calls")
                        && row.actorClass() == UsageActorClass.REGULAR
                        && row.valueSum() == 1));
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(
                snapshot ->
                    snapshot.metricName().equals("active_users")
                        && "regular".equals(snapshot.actorClass())));
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(snapshot -> snapshot.metricName().equals("active_writers")));
  }

  @Test
  public void testSystemActorIncludedInDistinctSets() {
    OperationContext opContext = session(Constants.SYSTEM_ACTOR, "metadata_write", null);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(
                snapshot ->
                    snapshot.metricName().equals("active_users")
                        && "system".equals(snapshot.actorClass())));
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(snapshot -> snapshot.metricName().equals("active_writers")));
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("api_calls")
                        && row.actorClass() == UsageActorClass.SYSTEM));
  }

  @Test
  public void testSupportActorIncludedInDistinctSets() {
    OperationContext opContext = session(SUPPORT_TEST_ACTOR, "metadata_read", null);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(
                snapshot ->
                    snapshot.metricName().equals("active_users")
                        && "support".equals(snapshot.actorClass())));
    Assert.assertTrue(
        batch.distinctSnapshots().stream()
            .anyMatch(snapshot -> snapshot.metricName().equals("active_readers")));
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("api_calls")
                        && row.actorClass() == UsageActorClass.SUPPORT));
  }

  @Test
  public void testDistinctIdentitiesBucketedByActorClass() {
    store.recordRequest(session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    store.recordRequest(session(SUPPORT_TEST_ACTOR, "metadata_read", null));
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertEquals(distinctIdentityCount(batch, "active_users", "regular"), 1L);
    Assert.assertEquals(distinctIdentityCount(batch, "active_users", "support"), 1L);
  }

  @Test
  public void testDistinctFlushRetainsIdentityListsPerActorClass() {
    Assert.assertTrue(
        store.recordRequest(
            session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null)));
    Assert.assertTrue(
        store.recordRequest(session("urn:li:corpuser:second-regular", "metadata_read", null)));
    store.flush(FlushTrigger.SCHEDULED);

    Assert.assertEquals(sink.batches().size(), 1);
    var batch = sink.batches().get(0);
    var activeUsers =
        batch.distinctSnapshots().stream()
            .filter(
                snapshot ->
                    snapshot.metricName().equals("active_users")
                        && "regular".equals(snapshot.actorClass()))
            .max(Comparator.comparingInt(snapshot -> snapshot.distinctCount()))
            .orElseThrow();
    Assert.assertEquals(activeUsers.distinctCount(), 2);
    Assert.assertEquals(
        activeUsers.usageIdentities().stream().sorted().toList(),
        java.util.List.of(UsageTestFixtures.REGULAR_CORP_USER_URN, "urn:li:corpuser:second-regular")
            .stream()
            .sorted()
            .toList());
  }

  @Test
  public void testScheduledFlushEmitsExplicitZeroIdentitySnapshots() {
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertEquals(batch.distinctSnapshots().size(), 9L);
    Assert.assertTrue(
        batch.distinctSnapshots().stream().allMatch(snapshot -> snapshot.identities().isEmpty()));

    SimpleMeterRegistry micrometerRegistry = new SimpleMeterRegistry();
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    new MicrometerUsageFlushSink(metricRegistry, micrometerRegistry).publish(batch);
    Assert.assertEquals(
        micrometerRegistry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "regular")
            .tag("identity_metric", "active_users")
            .gauge()
            .value(),
        0.0);
  }

  @Test
  public void testOtherWriteCountsAsActiveUserButNotWriter() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "other_write", null);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertEquals(distinctIdentityCount(batch, "active_users", "regular"), 1L);
    Assert.assertEquals(distinctIdentityCount(batch, "active_writers", "regular"), 0L);
  }

  @Test
  public void testOtherOperationsCountsAsReader() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "other_operations", null);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertEquals(distinctIdentityCount(batch, "active_readers", "regular"), 1L);
    Assert.assertEquals(distinctIdentityCount(batch, "active_writers", "regular"), 0L);
  }

  @Test
  public void testMetadataIngestInputBytesOmittedWhenWireBytesUnknown() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_ingest", null);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    long inputBytes =
        batch.additiveRows().stream()
            .filter(row -> row.metricName().equals("input_bytes"))
            .mapToLong(row -> row.valueSum())
            .sum();
    Assert.assertEquals(inputBytes, 0L);
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("api_calls")
                        && row.actorClass() == UsageActorClass.REGULAR));
  }

  @Test
  public void testMetadataIngestInputBytesFromMaterializedBody() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_ingest", 512L);
    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("input_bytes")
                        && row.actorClass() == UsageActorClass.REGULAR
                        && row.valueSum() == 512));
  }

  @Test
  public void testRecordResponseAddsOutputBytes() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", 50L);
    store.recordRequest(opContext);
    store.recordResponse(opContext, 200L);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("output_bytes")
                        && row.actorClass() == UsageActorClass.REGULAR
                        && row.valueSum() == 200));
  }

  @Test
  public void testRecordResponseSkipsUnknownUsageOperation() {
    OperationContext opContext =
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "not_a_real_operation", 50L);
    store.recordResponse(opContext, 200L);

    Assert.assertTrue(sink.batches().isEmpty());
  }

  @Test
  public void testRecordResponseSkipsDisallowedRequestApi() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.GRAPHQL)
            .requestID("testAction")
            .userAgent("test-agent")
            .usageOperation("metadata_ingest")
            .usageIdentity(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .authChannel(AuthChannel.SESSION)
            .inputBytes(50L)
            .requestBodyMaterialized(true)
            .build();
    Authentication authentication = userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN);
    OperationContext opContext;
    try {
      opContext =
          TestOperationContexts.systemContextNoValidate().toBuilder()
              .requestContext(requestContext)
              .build(authentication, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    store.recordResponse(opContext, 200L);

    Assert.assertTrue(sink.batches().isEmpty());
  }

  /**
   * Runs last under TestNG method ordering so a background flusher cannot leak into later tests.
   * Pins store/sink locals and awaits the flusher thread — required under {@code
   * parallel="classes"}.
   */
  @Test(priority = 100)
  public void testConcurrentRecordDuringFlushDoesNotLoseEvents() throws Exception {
    int requestCount = 200;
    InMemoryUsageAggregationStore concurrentStore = store;
    RecordingUsageFlushSink concurrentSink = sink;
    CountDownLatch recordersReady = new CountDownLatch(4);
    CountDownLatch startRecording = new CountDownLatch(1);
    CountDownLatch recordingDone = new CountDownLatch(4);
    CountDownLatch flushingDone = new CountDownLatch(1);
    AtomicInteger recorded = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(5);
    try {
      for (int t = 0; t < 4; t++) {
        executor.submit(
            () -> {
              recordersReady.countDown();
              try {
                startRecording.await();
                for (int i = 0; i < requestCount / 4; i++) {
                  if (concurrentStore.recordRequest(
                      session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_write", null))) {
                    recorded.incrementAndGet();
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                recordingDone.countDown();
              }
            });
      }

      recordersReady.await();
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < 50 && !Thread.currentThread().isInterrupted(); i++) {
                concurrentStore.flush(FlushTrigger.SCHEDULED);
                Thread.yield();
              }
            } finally {
              flushingDone.countDown();
            }
          });
      startRecording.countDown();
      Assert.assertTrue(recordingDone.await(30, TimeUnit.SECONDS));
      Assert.assertTrue(flushingDone.await(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    concurrentStore.flush(FlushTrigger.SHUTDOWN);

    long apiCalls =
        concurrentSink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(row -> row.valueSum())
            .sum();
    Assert.assertEquals(apiCalls, recorded.get());
  }

  @Test
  public void testConcurrentDistinctIdentityRecording() throws Exception {
    int threadCount = 8;
    int identitiesPerThread = 25;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        executor.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < identitiesPerThread; i++) {
                  store.recordRequest(
                      session(
                          "urn:li:corpuser:distinct-" + threadId + "-" + i, "metadata_read", null));
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      Assert.assertTrue(done.await(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    store.flush(FlushTrigger.SHUTDOWN);
    long activeUsers =
        sink.batches().stream()
            .flatMap(batch -> batch.distinctSnapshots().stream())
            .filter(
                snapshot ->
                    snapshot.metricName().equals("active_users")
                        && snapshot.actorClass().equals("regular"))
            .mapToLong(snapshot -> snapshot.identities().size())
            .sum();
    Assert.assertEquals(activeUsers, (long) threadCount * identitiesPerThread);
  }

  @Test
  public void testCardinalityDrainDoesNotBlockConcurrentRecording() throws Exception {
    sink = new RecordingUsageFlushSink();
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    InMemoryUsageAggregationStore lowCardinalityStore =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, deterministicActorClassResolver(), sink, 3, 300);

    CountDownLatch recordersReady = new CountDownLatch(4);
    CountDownLatch startRecording = new CountDownLatch(1);
    CountDownLatch recordingDone = new CountDownLatch(4);
    AtomicInteger recorded = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(5);
    try {
      for (int t = 0; t < 4; t++) {
        final int threadId = t;
        executor.submit(
            () -> {
              recordersReady.countDown();
              try {
                startRecording.await();
                for (int i = 0; i < 20; i++) {
                  if (lowCardinalityStore.recordRequest(
                      session(
                          "urn:li:corpuser:card-" + threadId + "-" + i, "metadata_write", null))) {
                    recorded.incrementAndGet();
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                recordingDone.countDown();
              }
            });
      }
      recordersReady.await();
      executor.submit(
          () -> {
            for (int i = 0; i < 30; i++) {
              lowCardinalityStore.flush(FlushTrigger.SCHEDULED);
              Thread.yield();
            }
          });
      startRecording.countDown();
      Assert.assertTrue(recordingDone.await(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    lowCardinalityStore.flush(FlushTrigger.SHUTDOWN);
    long apiCalls =
        sink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    Assert.assertEquals(apiCalls, recorded.get());
  }

  @Test
  public void testTryDrainSkipsWhenDrainInProgress() throws Exception {
    sink = new RecordingUsageFlushSink();
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    InMemoryUsageAggregationStore lowCardinalityStore =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, deterministicActorClassResolver(), sink, 2, 300);

    CountDownLatch flushStarted = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    RecordingUsageFlushSink slowSink =
        new RecordingUsageFlushSink() {
          @Override
          public void publish(@Nonnull UsageFlushBatch batch) {
            flushStarted.countDown();
            try {
              Assert.assertTrue(releaseFlush.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            super.publish(batch);
          }
        };
    lowCardinalityStore =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, deterministicActorClassResolver(), slowSink, 2, 300);
    final InMemoryUsageAggregationStore blockingStore = lowCardinalityStore;

    blockingStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(() -> blockingStore.flush(FlushTrigger.SCHEDULED));
      Assert.assertTrue(flushStarted.await(10, TimeUnit.SECONDS));

      blockingStore.recordRequest(session("urn:li:corpuser:other", "metadata_read", null));

      releaseFlush.countDown();
    } finally {
      executor.shutdownNow();
    }

    blockingStore.flush(FlushTrigger.SHUTDOWN);
    long apiCalls =
        slowSink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    Assert.assertEquals(apiCalls, 2L);
  }

  @Test
  public void testIsWindowExpiredReflectsMaxWindowSeconds() throws InterruptedException {
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    InMemoryUsageAggregationStore shortWindowStore =
        new InMemoryUsageAggregationStore(
            usageRegistry, metricRegistry, deterministicActorClassResolver(), sink, 10_000, 1L);
    Assert.assertFalse(shortWindowStore.isWindowExpired());
    Thread.sleep(1100L);
    Assert.assertTrue(shortWindowStore.isWindowExpired());
  }

  @Test
  public void testEmptyScheduledFlushPublishesZeroSnapshotsAndAdvancesWindow() {
    Instant before = store.windowStartSnapshot();
    store.flush(FlushTrigger.SCHEDULED);
    Assert.assertTrue(store.windowStartSnapshot().isAfter(before));
    Assert.assertEquals(sink.batches().size(), 1);
    Assert.assertTrue(
        sink.batches().get(0).distinctSnapshots().stream()
            .allMatch(snapshot -> snapshot.identities().isEmpty()));
  }

  @Test
  public void testPublishRetriesThenSucceeds() {
    AtomicInteger attempts = new AtomicInteger();
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    UsageFlushSink retryingSink =
        batch -> {
          if (attempts.incrementAndGet() < 2) {
            throw new IllegalStateException("transient failure");
          }
          recordingSink.publish(batch);
        };
    InMemoryUsageAggregationStore retryStore = storeWithSink(retryingSink, 3, 0L);

    retryStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    retryStore.flush(FlushTrigger.SCHEDULED);

    Assert.assertEquals(attempts.get(), 2);
    Assert.assertEquals(recordingSink.batches().size(), 1);
    long apiCalls =
        recordingSink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(row -> row.valueSum())
            .sum();
    Assert.assertEquals(apiCalls, 1L);
  }

  @Test
  public void testPublishFailureRemergesIntoOriginalWindow() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    AtomicInteger attempts = new AtomicInteger();
    UsageFlushSink sink =
        batch -> {
          if (attempts.getAndIncrement() == 0) {
            throw new IllegalStateException("publish failed");
          }
          recordingSink.publish(batch);
        };
    InMemoryUsageAggregationStore retryStore = storeWithSink(sink, 1, 0L);

    Instant windowBeforeRecord = retryStore.windowStartSnapshot();
    retryStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    Assert.assertEquals(retryStore.windowStartSnapshot(), windowBeforeRecord);

    retryStore.flush(FlushTrigger.SCHEDULED);
    Assert.assertTrue(retryStore.windowStartSnapshot().isAfter(windowBeforeRecord));
    Assert.assertTrue(recordingSink.batches().isEmpty());
    Assert.assertTrue(retryStore.currentCardinality() > 0);

    retryStore.flush(FlushTrigger.SCHEDULED);
    Assert.assertEquals(recordingSink.batches().size(), 1);
    long apiCalls =
        recordingSink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    Assert.assertEquals(apiCalls, 1L);
    Assert.assertEquals(
        distinctIdentityCount(recordingSink.batches().get(0), "active_users", "regular"), 1L);
  }

  @Test
  public void testPublishFailureAfterRetriesRemergesData() {
    AtomicInteger attempts = new AtomicInteger();
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    UsageFlushSink sink =
        batch -> {
          if (attempts.incrementAndGet() <= 2) {
            throw new IllegalStateException("publish failed");
          }
          recordingSink.publish(batch);
        };
    InMemoryUsageAggregationStore retryStore = storeWithSink(sink, 2, 0L);

    retryStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    retryStore.flush(FlushTrigger.SCHEDULED);
    Assert.assertEquals(attempts.get(), 2);
    Assert.assertTrue(recordingSink.batches().isEmpty());

    retryStore.recordRequest(
        session(UsageTestFixtures.REGULAR_CORP_USER_URN, "metadata_read", null));
    retryStore.flush(FlushTrigger.SCHEDULED);

    Assert.assertEquals(attempts.get(), 3);
    Assert.assertEquals(recordingSink.batches().size(), 1);
    long apiCalls =
        recordingSink.batches().stream()
            .flatMap(batch -> batch.additiveRows().stream())
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(row -> row.valueSum())
            .sum();
    Assert.assertEquals(apiCalls, 2L);
    Assert.assertEquals(
        distinctIdentityCount(recordingSink.batches().get(0), "active_users", "regular"), 1L);
  }

  private InMemoryUsageAggregationStore storeWithSink(
      UsageFlushSink flushSink, int retryAttempts, long retryBackoffMillis) {
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper()), java.util.List.of());
    return new InMemoryUsageAggregationStore(
        usageRegistry,
        metricRegistry,
        deterministicActorClassResolver(),
        flushSink,
        10_000,
        300,
        retryAttempts,
        retryBackoffMillis);
  }

  @Test
  public void testMetadataIngestScalesByUsageQuantity() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.RESTLI)
            .requestID("batchIngest")
            .userAgent("test-agent")
            .usageOperation("metadata_ingest")
            .usageIdentity(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .authChannel(AuthChannel.SESSION)
            .usageQuantity(5)
            .build();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");
    OperationContext opContext;
    try {
      opContext =
          TestOperationContexts.systemContextNoValidate().toBuilder()
              .requestContext(requestContext)
              .build(authentication, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    store.recordRequest(opContext);
    store.flush(FlushTrigger.SCHEDULED);

    var batch = sink.batches().get(0);
    Assert.assertTrue(
        batch.additiveRows().stream()
            .anyMatch(
                row ->
                    row.metricName().equals("api_calls")
                        && row.actorClass() == UsageActorClass.REGULAR
                        && row.valueSum() == 5));
  }

  private static long distinctIdentityCount(
      UsageFlushBatch batch, String metricName, String actorClass) {
    return batch.distinctSnapshots().stream()
        .filter(
            snapshot ->
                snapshot.metricName().equals(metricName)
                    && actorClass.equals(snapshot.actorClass()))
        .mapToLong(snapshot -> snapshot.identities().size())
        .max()
        .orElse(-1L);
  }

  private static OperationContext session(String actorUrn, String usageOperation, Long inputBytes) {
    return session(actorUrn, usageOperation, inputBytes, AuthChannel.SESSION);
  }

  private static OperationContext session(
      String actorUrn, String usageOperation, Long inputBytes, AuthChannel authChannel) {
    // Drop any ThreadLocal actor-class / corp-user flag memoization left by prior tests on this
    // worker thread before building a fresh session context.
    UsageRequestState.clear();
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(actorUrn)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("testAction")
            .userAgent("test-agent")
            .usageOperation(usageOperation)
            .usageIdentity(actorUrn)
            .authChannel(authChannel)
            .inputBytes(inputBytes)
            .requestBodyMaterialized(inputBytes != null)
            .build();
    Authentication authentication = userAuth(actorUrn);
    OperationContext base = TestOperationContexts.systemContextNoValidate();
    try {
      return base.toBuilder().requestContext(requestContext).build(authentication, true);
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
}
