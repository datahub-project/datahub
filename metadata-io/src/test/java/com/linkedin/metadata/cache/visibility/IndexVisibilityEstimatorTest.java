package com.linkedin.metadata.cache.visibility;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.cache.visibility.IndexVisibilityEstimator.IndexWriteSettings;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.RefreshIntervals;
import java.util.EnumMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class IndexVisibilityEstimatorTest {

  private static BulkProcessorConfiguration.BulkProcessorConfigurationBuilder bulk() {
    return BulkProcessorConfiguration.builder()
        .flushPeriod(1)
        .numRetries(0)
        .retryInterval(0)
        .refreshPolicy("NONE");
  }

  private static IndexVisibilityEstimator estimator(
      Map<SourceIndexKind, IndexWriteSettings> settings, IndexWriteFlusher flusher) {
    return new IndexVisibilityEstimator(
        kind -> settings.getOrDefault(kind, new IndexWriteSettings(null, null)), flusher);
  }

  private static long boundSeconds(OptionalLong visibleAt, long before) {
    assertTrue(visibleAt.isPresent());
    return Math.round((visibleAt.getAsLong() - before) / 1000.0);
  }

  @Test
  public void submitBoundAddsFlushRetriesRequeueRefreshAndMargin() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.GRAPH,
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(2).build(),
            bulk()
                .flushPeriod(1)
                .numRetries(2)
                .retryInterval(3)
                .itemRequeueEnabled(true)
                .itemRequeueMaxAttempts(1)
                .build()));

    long before = System.currentTimeMillis();
    OptionalLong visibleAt =
        estimator(settings, IndexWriteFlusher.NONE)
            .newSession()
            .visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.SUBMIT_BOUND, 1);

    // (flush 1 + 2 retries * 3s) * (1 + 1 requeue) + refresh 2 + margin 1
    assertEquals(boundSeconds(visibleAt, before), 17);
  }

  @Test
  public void entityKindUsesEntityOverrideThenEntitySeconds() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.ENTITY,
        new IndexWriteSettings(
            RefreshIntervals.builder().entitySeconds(5).entities("{\"domain\": 9}").build(),
            bulk().flushPeriod(0).build()));
    IndexVisibilityEstimator estimator = estimator(settings, IndexWriteFlusher.NONE);

    long before = System.currentTimeMillis();
    assertEquals(
        boundSeconds(
            estimator
                .newSession()
                .visibleAtMillis(SourceIndexKind.ENTITY, "domain", VisibilityMode.SUBMIT_BOUND, 0),
            before),
        9);
    assertEquals(
        boundSeconds(
            estimator
                .newSession()
                .visibleAtMillis(SourceIndexKind.ENTITY, "dataset", VisibilityMode.SUBMIT_BOUND, 0),
            before),
        5);
  }

  @Test
  public void flushAwaitFlushesEachKindOnceAndDropsTheSubmitBound() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.GRAPH,
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(2).build(), bulk().flushPeriod(10).build()));
    AtomicInteger flushes = new AtomicInteger();
    IndexVisibilityEstimator estimator =
        estimator(
            settings,
            kind -> {
              flushes.incrementAndGet();
              return true;
            });
    IndexVisibilityEstimator.Session session = estimator.newSession();

    long before = System.currentTimeMillis();
    OptionalLong first =
        session.visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.FLUSH_AWAIT, 0);
    session.visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.FLUSH_AWAIT, 0);

    assertEquals(flushes.get(), 1);
    assertEquals(boundSeconds(first, before), 2);
  }

  @Test
  public void failedFlushFallsBackToTheSubmitBound() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.GRAPH,
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(2).build(), bulk().flushPeriod(4).build()));

    long before = System.currentTimeMillis();
    OptionalLong visibleAt =
        estimator(
                settings,
                kind -> {
                  throw new IllegalStateException("cluster unavailable");
                })
            .newSession()
            .visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.FLUSH_AWAIT, 0);

    assertEquals(boundSeconds(visibleAt, before), 6);
  }

  @Test
  public void immediateRefreshPolicyLeavesOnlyTheSubmitBound() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.GRAPH,
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(30).build(),
            bulk().flushPeriod(1).refreshPolicy("WAIT_FOR").build()));

    long before = System.currentTimeMillis();
    OptionalLong visibleAt =
        estimator(settings, IndexWriteFlusher.NONE)
            .newSession()
            .visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.SUBMIT_BOUND, 0);

    assertEquals(boundSeconds(visibleAt, before), 1);
  }

  @Test
  public void unusableRefreshIntervalProducesNoBound() {
    Map<SourceIndexKind, IndexWriteSettings> settings = new EnumMap<>(SourceIndexKind.class);
    settings.put(
        SourceIndexKind.GRAPH,
        new IndexWriteSettings(
            RefreshIntervals.builder().graphSeconds(-1).build(), bulk().build()));
    IndexVisibilityEstimator estimator = estimator(settings, IndexWriteFlusher.NONE);

    assertFalse(
        estimator
            .newSession()
            .visibleAtMillis(SourceIndexKind.GRAPH, null, VisibilityMode.SUBMIT_BOUND, 0)
            .isPresent());
    assertFalse(
        estimator
            .newSession()
            .visibleAtMillis(SourceIndexKind.ENTITY, "domain", VisibilityMode.SUBMIT_BOUND, 0)
            .isPresent());
  }
}
