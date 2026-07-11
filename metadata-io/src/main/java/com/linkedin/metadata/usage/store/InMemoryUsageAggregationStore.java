package com.linkedin.metadata.usage.store;

import com.linkedin.metadata.usage.UsageDimensions;
import com.linkedin.metadata.usage.flush.AdditiveUsageRow;
import com.linkedin.metadata.usage.flush.DistinctIdentityEntry;
import com.linkedin.metadata.usage.flush.DistinctIdentitySet;
import com.linkedin.metadata.usage.flush.DistinctUsageSnapshot;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.flush.UsageFlushBatch;
import com.linkedin.metadata.usage.flush.UsageFlushBoundaryUtils;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricIncrementResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.ActivityClass;
import com.linkedin.metadata.usage.registry.operations.ActivitySnapshot;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.utils.retry.ExponentialBackoffRetry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AttributionType;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory {@link UsageAggregationStore} for GMS API request/response usage metrics.
 *
 * <p>Renamed from {@code InMemoryUsageRollupStore} to avoid colliding with the legacy product-usage
 * rollup type in {@code com.linkedin.metadata.billing.rollup}.
 *
 * <p>Recorders hold a shared read lock while writing to the active window; drain swaps windows
 * under an exclusive write lock (brief) then builds and publishes Micrometer batches outside any
 * lock.
 */
@Slf4j
public class InMemoryUsageAggregationStore implements UsageAggregationStore {

  private static final ZoneOffset ALIGNMENT_ZONE = ZoneOffset.UTC;

  private final UsageOperationsRegistry usageOperationsRegistry;
  private final UsageMetricRegistry metricRegistry;
  private final UsageActorClassResolver actorClassResolver;
  private final UsageFlushSink flushSink;
  private final int maxCardinality;
  private final long maxWindowMillis;
  private final int retryAttempts;
  private final long retryInitialBackoffMillis;
  @Nullable private final Duration alignmentPeriod;
  @Nonnull private final Clock clock;

  /** Protects active-window swap; concurrent recorders share the read lock. */
  private final ReentrantReadWriteLock windowLock = new ReentrantReadWriteLock();

  private volatile ActiveWindow activeWindow;

  public InMemoryUsageAggregationStore(
      @Nonnull UsageOperationsRegistry usageOperationsRegistry,
      @Nonnull UsageMetricRegistry metricRegistry,
      @Nonnull UsageActorClassResolver actorClassResolver,
      @Nonnull UsageFlushSink flushSink,
      int maxCardinality,
      long maxWindowSeconds) {
    this(
        usageOperationsRegistry,
        metricRegistry,
        actorClassResolver,
        flushSink,
        maxCardinality,
        maxWindowSeconds,
        3,
        100L);
  }

  public InMemoryUsageAggregationStore(
      @Nonnull UsageOperationsRegistry usageOperationsRegistry,
      @Nonnull UsageMetricRegistry metricRegistry,
      @Nonnull UsageActorClassResolver actorClassResolver,
      @Nonnull UsageFlushSink flushSink,
      int maxCardinality,
      long maxWindowSeconds,
      int retryAttempts,
      long retryInitialBackoffMillis) {
    this(
        usageOperationsRegistry,
        metricRegistry,
        actorClassResolver,
        flushSink,
        maxCardinality,
        maxWindowSeconds,
        retryAttempts,
        retryInitialBackoffMillis,
        null,
        Clock.systemUTC());
  }

  public InMemoryUsageAggregationStore(
      @Nonnull UsageOperationsRegistry usageOperationsRegistry,
      @Nonnull UsageMetricRegistry metricRegistry,
      @Nonnull UsageActorClassResolver actorClassResolver,
      @Nonnull UsageFlushSink flushSink,
      int maxCardinality,
      long maxWindowSeconds,
      int retryAttempts,
      long retryInitialBackoffMillis,
      long alignmentPeriodSeconds) {
    this(
        usageOperationsRegistry,
        metricRegistry,
        actorClassResolver,
        flushSink,
        maxCardinality,
        maxWindowSeconds,
        retryAttempts,
        retryInitialBackoffMillis,
        alignmentPeriodSeconds,
        Clock.systemUTC());
  }

  public InMemoryUsageAggregationStore(
      @Nonnull UsageOperationsRegistry usageOperationsRegistry,
      @Nonnull UsageMetricRegistry metricRegistry,
      @Nonnull UsageActorClassResolver actorClassResolver,
      @Nonnull UsageFlushSink flushSink,
      int maxCardinality,
      long maxWindowSeconds,
      int retryAttempts,
      long retryInitialBackoffMillis,
      @Nullable Long alignmentPeriodSeconds,
      @Nonnull Clock clock) {
    this.usageOperationsRegistry = usageOperationsRegistry;
    this.metricRegistry = metricRegistry;
    this.actorClassResolver = actorClassResolver;
    this.flushSink = flushSink;
    this.maxCardinality = maxCardinality;
    this.maxWindowMillis = maxWindowSeconds * 1000L;
    this.retryAttempts = Math.max(1, retryAttempts);
    this.retryInitialBackoffMillis = Math.max(0, retryInitialBackoffMillis);
    this.alignmentPeriod =
        alignmentPeriodSeconds != null && alignmentPeriodSeconds > 0
            ? Duration.ofSeconds(alignmentPeriodSeconds)
            : null;
    this.clock = clock;
    this.activeWindow = newActiveWindow(null);
  }

  @Override
  public boolean recordRequest(@Nonnull OperationContext opContext) {
    RequestContext requestContext = opContext.getRequestContext();
    UsageOperationsRegistry.UsageOperationEntry operationEntry =
        resolveAllowedOperation(requestContext);
    if (operationEntry == null) {
      return false;
    }
    UsageActorClass actorClass =
        actorClassResolver.resolve(opContext, requestContext, opContext.getSessionAuthentication());
    String usageIdentity =
        requestContext.getUsageIdentity() != null
            ? requestContext.getUsageIdentity()
            : requestContext.getActorUrn();
    AttributionType attribution = UsageDimensions.resolveAttribution(requestContext);
    ActivityClass activityClass = operationEntry.activityClass();

    Map<String, String> dimensions =
        UsageDimensions.fromRequestContext(
            requestContext, requestContext.getUsageOperation(), actorClass.dimensionValue());

    ActivitySnapshot activitySnapshot = ActivitySnapshot.fromActivityClass(activityClass);

    windowLock.readLock().lock();
    try {
      ActiveWindow window = activeWindow;
      String windowId = window.windowId();
      for (UsageMetricRegistry.MetricDefinition metric :
          metricRegistry.apiUsageMetrics().values()) {
        if (metric.mergeKind() == UsageMetricRegistry.MergeKind.DISTINCT) {
          if (!UsageMetricIncrementResolver.shouldEmitDistinct(
              metric, activitySnapshot, operationEntry)) {
            continue;
          }
          DistinctRollupKey key =
              new DistinctRollupKey(windowId, metric.metricName(), actorClass.dimensionValue());
          recordDistinctIdentity(window, key, usageIdentity, attribution);
          continue;
        }

        long increment =
            UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
                metric, operationEntry, requestContext);
        if (increment <= 0) {
          continue;
        }
        AdditiveRollupKey key =
            new AdditiveRollupKey(windowId, metric.metricName(), actorClass, dimensions);
        recordAdditive(window, key, increment);
      }
    } finally {
      windowLock.readLock().unlock();
    }
    tryTriggerDrain();
    return true;
  }

  @Override
  public void recordResponse(@Nonnull OperationContext opContext, @Nullable Long outputBytes) {
    RequestContext requestContext = opContext.getRequestContext();
    if (resolveAllowedOperation(requestContext) == null) {
      return;
    }
    if (outputBytes == null || outputBytes <= 0) {
      return;
    }
    UsageActorClass actorClass =
        actorClassResolver.resolve(opContext, requestContext, opContext.getSessionAuthentication());

    windowLock.readLock().lock();
    try {
      ActiveWindow window = activeWindow;
      String windowId = window.windowId();
      Map<String, String> dimensions =
          UsageDimensions.fromRequestContext(
              requestContext, requestContext.getUsageOperation(), actorClass.dimensionValue());
      for (UsageMetricRegistry.MetricDefinition metric :
          metricRegistry.apiUsageMetrics().values()) {
        long increment =
            UsageMetricIncrementResolver.resolveResponsePhaseIncrement(metric, outputBytes);
        if (increment <= 0) {
          continue;
        }
        AdditiveRollupKey key =
            new AdditiveRollupKey(windowId, metric.metricName(), actorClass, dimensions);
        recordAdditive(window, key, increment);
      }
    } finally {
      windowLock.readLock().unlock();
    }
    tryTriggerDrain();
  }

  @Override
  public void flush(@Nonnull FlushTrigger trigger) {
    if (alignmentPeriod == null) {
      publishBatch(swapAndExtractBlocking(trigger), trigger);
      return;
    }
    while (true) {
      Instant now = clock.instant();
      Instant windowStart = windowStartSnapshot();
      Instant boundary =
          UsageFlushBoundaryUtils.nextBoundary(windowStart, alignmentPeriod, ALIGNMENT_ZONE);
      boolean splitAtBoundary = !now.isBefore(boundary);
      Instant batchEnd = splitAtBoundary ? boundary : now;

      ActiveWindow retired;
      windowLock.writeLock().lock();
      try {
        retired = swapActiveWindowLocked(splitAtBoundary ? boundary : null);
      } finally {
        windowLock.writeLock().unlock();
      }
      publishBatch(buildBatchFrom(retired, trigger, batchEnd), trigger);

      if (!splitAtBoundary) {
        return;
      }
      if (clock.instant().isBefore(nextAlignmentBoundary(activeWindow.windowStart))) {
        return;
      }
    }
  }

  public int currentCardinality() {
    return activeWindow.approximateCardinality.get();
  }

  public boolean isWindowExpired() {
    ActiveWindow window = activeWindow;
    return clock.instant().toEpochMilli() - window.windowStart.toEpochMilli() >= maxWindowMillis;
  }

  public boolean isAlignmentEnabled() {
    return alignmentPeriod != null;
  }

  @Nullable
  public Duration alignmentPeriod() {
    return alignmentPeriod;
  }

  @Nonnull
  public Clock clock() {
    return clock;
  }

  /** Visible for flush coordination and tests. */
  public Instant windowStartSnapshot() {
    return activeWindow.windowStart;
  }

  @Nullable
  private UsageOperationsRegistry.UsageOperationEntry resolveAllowedOperation(
      @Nonnull RequestContext requestContext) {
    String usageOperationKey = requestContext.getUsageOperation();
    if (usageOperationKey == null) {
      return null;
    }
    try {
      UsageOperationsRegistry.UsageOperationEntry operationEntry =
          usageOperationsRegistry.require(usageOperationKey);
      if (!usageOperationsRegistry.isAllowedForApi(
          operationEntry, requestContext.getRequestAPI())) {
        return null;
      }
      return operationEntry;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** Non-blocking cardinality drain; skips when another thread is swapping windows. */
  private void tryTriggerDrain() {
    if (maxCardinality <= 0 || activeWindow.approximateCardinality.get() < maxCardinality) {
      return;
    }
    ActiveWindow retired = null;
    if (windowLock.writeLock().tryLock()) {
      try {
        if (activeWindow.approximateCardinality.get() >= maxCardinality) {
          retired = swapActiveWindowLocked();
        }
      } finally {
        windowLock.writeLock().unlock();
      }
    }
    if (retired != null) {
      publishExtractedWindow(retired, FlushTrigger.CARDINALITY);
    }
  }

  private void publishExtractedWindow(
      @Nonnull ActiveWindow retired, @Nonnull FlushTrigger trigger) {
    Instant now = clock.instant();
    if (alignmentPeriod == null) {
      publishBatch(buildBatchFrom(retired, trigger, now), trigger);
      return;
    }
    Instant boundary = nextAlignmentBoundary(retired.windowStart);
    if (now.isBefore(boundary)) {
      publishBatch(buildBatchFrom(retired, trigger, now), trigger);
      return;
    }
    publishBatch(buildBatchFrom(retired, trigger, boundary), trigger);
  }

  @Nonnull
  private Instant nextAlignmentBoundary(@Nonnull Instant windowStart) {
    return UsageFlushBoundaryUtils.nextBoundary(windowStart, alignmentPeriod, ALIGNMENT_ZONE);
  }

  @Nullable
  private UsageFlushBatch swapAndExtractBlocking(@Nonnull FlushTrigger trigger) {
    ActiveWindow retired;
    windowLock.writeLock().lock();
    try {
      retired = swapActiveWindowLocked();
    } finally {
      windowLock.writeLock().unlock();
    }
    return buildBatchFrom(retired, trigger, clock.instant());
  }

  /** Caller must hold {@link ReentrantReadWriteLock#writeLock()}. */
  @Nonnull
  private ActiveWindow swapActiveWindowLocked() {
    return swapActiveWindowLocked(null);
  }

  /** Caller must hold {@link ReentrantReadWriteLock#writeLock()}. */
  @Nonnull
  private ActiveWindow swapActiveWindowLocked(@Nullable Instant nextWindowStart) {
    ActiveWindow retired = activeWindow;
    activeWindow = newActiveWindow(nextWindowStart);
    return retired;
  }

  @Nonnull
  private ActiveWindow newActiveWindow(@Nullable Instant explicitStart) {
    Instant windowStart = explicitStart != null ? explicitStart : clock.instant();
    if (alignmentPeriod != null && explicitStart == null) {
      windowStart = UsageFlushBoundaryUtils.alignDown(windowStart, alignmentPeriod, ALIGNMENT_ZONE);
    }
    return new ActiveWindow(windowStart);
  }

  @Nullable
  private UsageFlushBatch buildBatchFrom(
      @Nonnull ActiveWindow window, @Nonnull FlushTrigger trigger) {
    return buildBatchFrom(window, trigger, clock.instant());
  }

  @Nullable
  private UsageFlushBatch buildBatchFrom(
      @Nonnull ActiveWindow window, @Nonnull FlushTrigger trigger, @Nonnull Instant windowEnd) {
    List<AdditiveUsageRow> additiveRows = drainAdditive(window.additiveBuckets);
    List<DistinctUsageSnapshot> distinctSnapshots = drainDistinct(window.distinctBuckets);
    if (additiveRows.isEmpty()
        && distinctSnapshots.stream().noneMatch(snapshot -> !snapshot.identities().isEmpty())
        && !shouldPublishDistinctOnlyBatch(trigger)) {
      return null;
    }
    return new UsageFlushBatch(
        window.windowStart, windowEnd, trigger, additiveRows, distinctSnapshots);
  }

  private List<AdditiveUsageRow> drainAdditive(
      ConcurrentHashMap<AdditiveRollupKey, LongAdder> buckets) {
    List<AdditiveUsageRow> rows = new ArrayList<>();
    for (var entry : buckets.entrySet()) {
      long sum = entry.getValue().sumThenReset();
      if (sum <= 0) {
        continue;
      }
      AdditiveRollupKey key = entry.getKey();
      rows.add(new AdditiveUsageRow(key.metricName(), key.actorClass(), key.dimensions(), sum));
    }
    buckets.clear();
    return rows;
  }

  private List<DistinctUsageSnapshot> drainDistinct(
      ConcurrentHashMap<DistinctRollupKey, DistinctIdentitySet> buckets) {
    Map<DistinctRollupKey, DistinctIdentitySet> drained = Map.copyOf(buckets);
    buckets.clear();

    Map<String, Map<String, DistinctIdentitySet>> byMetricAndActorClass = new HashMap<>();
    for (var entry : drained.entrySet()) {
      DistinctRollupKey key = entry.getKey();
      byMetricAndActorClass
          .computeIfAbsent(key.metricName(), ignored -> new HashMap<>())
          .merge(
              key.actorClass(),
              entry.getValue(),
              (existing, incoming) -> {
                existing.mergeFrom(incoming.toEntries());
                return existing;
              });
    }

    List<DistinctUsageSnapshot> snapshots = new ArrayList<>();
    for (UsageMetricRegistry.MetricDefinition metric : metricRegistry.apiUsageMetrics().values()) {
      if (metric.mergeKind() != UsageMetricRegistry.MergeKind.DISTINCT) {
        continue;
      }
      Map<String, DistinctIdentitySet> byActorClass =
          byMetricAndActorClass.getOrDefault(metric.metricName(), Map.of());
      for (UsageActorClass actorClass : UsageActorClass.values()) {
        DistinctIdentitySet identitySet = byActorClass.get(actorClass.dimensionValue());
        List<DistinctIdentityEntry> entries =
            identitySet != null ? identitySet.toEntries() : List.of();
        snapshots.add(
            new DistinctUsageSnapshot(metric.metricName(), actorClass.dimensionValue(), entries));
      }
    }
    return snapshots;
  }

  private void recordDistinctIdentity(
      @Nonnull ActiveWindow window,
      @Nonnull DistinctRollupKey key,
      @Nonnull String usageIdentity,
      @Nonnull AttributionType attribution) {
    DistinctIdentitySet identities =
        window.distinctBuckets.computeIfAbsent(
            key,
            ignored -> {
              window.approximateCardinality.incrementAndGet();
              return new DistinctIdentitySet();
            });
    if (identities.add(usageIdentity, attribution)) {
      window.approximateCardinality.incrementAndGet();
    }
  }

  private void recordAdditive(
      @Nonnull ActiveWindow window, @Nonnull AdditiveRollupKey key, long increment) {
    LongAdder adder =
        window.additiveBuckets.computeIfAbsent(
            key,
            ignored -> {
              window.approximateCardinality.incrementAndGet();
              return new LongAdder();
            });
    adder.add(increment);
  }

  private boolean shouldPublishDistinctOnlyBatch(@Nonnull FlushTrigger trigger) {
    return trigger == FlushTrigger.SCHEDULED
        || trigger == FlushTrigger.MAX_WINDOW
        || trigger == FlushTrigger.SHUTDOWN;
  }

  private void publishBatch(@Nullable UsageFlushBatch batch, @Nonnull FlushTrigger trigger) {
    if (batch == null) {
      return;
    }
    if (publishWithRetry(batch, trigger)) {
      return;
    }
    log.error(
        "Re-merging usage flush batch after {} failed publish attempts trigger={} additiveRows={}"
            + " distinctSnapshots={}",
        retryAttempts,
        trigger,
        batch.additiveRows().size(),
        batch.distinctSnapshots().size());
    remergeBatch(batch, trigger);
  }

  private boolean publishWithRetry(@Nonnull UsageFlushBatch batch, @Nonnull FlushTrigger trigger) {
    boolean published =
        ExponentialBackoffRetry.run(
            retryAttempts,
            retryInitialBackoffMillis,
            () -> flushSink.publish(batch),
            (attempt, failure) ->
                log.warn(
                    "Failed to publish usage flush batch attempt {}/{} trigger={} additiveRows={}"
                        + " distinctSnapshots={}",
                    attempt,
                    retryAttempts,
                    trigger,
                    batch.additiveRows().size(),
                    batch.distinctSnapshots().size(),
                    failure));
    if (published) {
      log.debug(
          "Flushed usage batch trigger={} additiveRows={} distinctSnapshots={}",
          trigger,
          batch.additiveRows().size(),
          batch.distinctSnapshots().size());
    }
    return published;
  }

  private void remergeBatch(@Nonnull UsageFlushBatch batch, @Nonnull FlushTrigger trigger) {
    windowLock.writeLock().lock();
    try {
      ActiveWindow window = activeWindow;
      String windowId = window.windowId();
      for (AdditiveUsageRow row : batch.additiveRows()) {
        AdditiveRollupKey key =
            new AdditiveRollupKey(windowId, row.metricName(), row.actorClass(), row.dimensions());
        recordAdditive(window, key, row.valueSum());
      }
      for (DistinctUsageSnapshot snapshot : batch.distinctSnapshots()) {
        DistinctRollupKey key =
            new DistinctRollupKey(windowId, snapshot.metricName(), snapshot.actorClass());
        for (DistinctIdentityEntry identity : snapshot.identities()) {
          recordDistinctIdentity(window, key, identity.usageIdentity(), identity.attributionType());
        }
      }
    } finally {
      windowLock.writeLock().unlock();
    }
  }

  static final class ActiveWindow {
    final Instant windowStart;
    final ConcurrentHashMap<AdditiveRollupKey, LongAdder> additiveBuckets =
        new ConcurrentHashMap<>();
    final ConcurrentHashMap<DistinctRollupKey, DistinctIdentitySet> distinctBuckets =
        new ConcurrentHashMap<>();
    final AtomicInteger approximateCardinality = new AtomicInteger(0);

    ActiveWindow(@Nonnull Instant windowStart) {
      this.windowStart = windowStart;
    }

    @Nonnull
    String windowId() {
      return windowStart.toString();
    }
  }
}
