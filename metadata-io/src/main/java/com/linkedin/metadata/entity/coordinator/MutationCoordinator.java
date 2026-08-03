package com.linkedin.metadata.entity.coordinator;

import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Pure orchestration for the coordinated-ingest COORDINATE stage. Best-effort acquires a lease per
 * conflict key via a pluggable {@link CoordinationLockProvider}, then <b>always</b> runs the
 * coordinated commit. The DB single-sorted {@code FOR UPDATE} in the commit is the authoritative
 * correctness mechanism, so the commit runs whether or not the provider lock was acquired.
 *
 * <p>Conflict keys are derived once, up front, from the INPUT batch (see {@code
 * EntityServiceImpl.buildCoordinationPlan}) — the Hazelcast lease only serializes contending
 * commands before they reach the DB. Side-effect rows discovered later under the DB lock are
 * covered by the commit's single-wave sorted {@code FOR UPDATE} (the cross-transaction correctness
 * floor), not by the Hazelcast lock. This is sufficient for schemaField, whose side effects share
 * the parent-dataset conflict key.
 *
 * <p>This class never falls back to the legacy path; that decision belongs to the caller and only
 * when the coordinated-ingest feature flag is off. It also holds no dependency on {@code
 * EntityServiceImpl}: the plan is supplied by the caller and the transaction by {@link
 * CoordinatedCommit}.
 *
 * <p>The lock substrate is fully pluggable via {@link CoordinationLockProvider} (Hazelcast, a
 * future Redis provider, or an in-JVM local lock) and env-selected in the factory; correctness
 * never depends on which provider — or whether one is present at all.
 *
 * <p>Degradation: when {@code lockProvider} is {@code null} (no substrate configured/available) the
 * coordinator is a no-op pass-through that commits directly. Correctness still holds via the DB
 * sorted commit.
 *
 * <p>Thread-safety: instances are stateless aside from immutable dependencies and may be shared.
 */
@Slf4j
public class MutationCoordinator {

  // TODO(coordinated-ingest observability): confirm final metric names with the shadow-metrics
  // phase. These mirror the counters listed in the design doc's v1 shadow-metrics section.
  static final String METRIC_LOCK_TIMEOUTS = "coordinator.lock_timeouts";
  // Distribution (histogram) of the per-plan conflict-key count -- NOT a cumulative counter, so
  // plan sizes are observable without accumulating across plans.
  static final String METRIC_PLAN_CONFLICT_KEYS = "coordinator.plan_conflict_keys";
  // Counter: the commit-under-lock outran the IMap lease, so the lease silently expired and a
  // second
  // writer could have entered. Correctness still holds via the DB sorted commit; this only surfaces
  // the otherwise-invisible window.
  static final String METRIC_LEASE_EXCEEDED = "coordinator.lease_exceeded";

  @Nullable private final CoordinationLockProvider lockProvider;
  @Nonnull private final CoordinatedIngestConfiguration config;
  @Nullable private final MetricUtils metricUtils;

  /**
   * @param lockProvider the best-effort lock substrate, or {@code null} to disable distributed
   *     locking entirely (no-op pass-through commit)
   * @param config coordinated-ingest tunables (lock lease, acquire timeout)
   * @param metricUtils sink for coordinator counters, or {@code null} to skip metrics
   */
  public MutationCoordinator(
      @Nullable final CoordinationLockProvider lockProvider,
      @Nonnull final CoordinatedIngestConfiguration config,
      @Nullable final MetricUtils metricUtils) {
    this.lockProvider = lockProvider;
    this.config = config;
    this.metricUtils = metricUtils;
  }

  /**
   * Coordinates and commits a single mutation command.
   *
   * <ol>
   *   <li>If no lock provider is configured, commit directly (DB lock still authoritative).
   *   <li>Otherwise acquire a lease per conflict key in sorted order (best-effort; lock timeouts
   *       are recorded but never block progress).
   *   <li>Run the commit under whatever locks were held, then release all held locks in reverse
   *       order.
   * </ol>
   *
   * @return the commit result
   * @throws Exception whatever {@link CoordinatedCommit#commitUnderLock} throws
   */
  public <T> T execute(@Nonnull final MutationPlan plan, @Nonnull final CoordinatedCommit<T> commit)
      throws Exception {
    recordPlanConflictKeys(plan.conflictKeys().size());

    if (lockProvider == null) {
      // No lock substrate: no-op locking. The DB single-sorted commit remains authoritative.
      return commit.commitUnderLock(plan);
    }

    final List<String> heldLocks = new ArrayList<>();
    try {
      acquireLocks(plan.conflictKeys(), heldLocks);
      // Lease clock effectively starts once locks are held; measure the commit against it to
      // surface
      // silent lease expiry (a long tx outrunning the lease lets a second writer in -- DB stays
      // authoritative, but the window is otherwise invisible).
      final long commitStartMs = System.currentTimeMillis();
      final T result = commit.commitUnderLock(plan);
      recordLeaseExceeded(commitStartMs, heldLocks.isEmpty());
      return result;
    } finally {
      releaseLocks(heldLocks);
    }
  }

  /**
   * Best-effort acquisition of a lease per conflict key, in the {@link java.util.SortedSet}'s
   * natural (sorted) order to avoid distributed lock-order inversion. A timeout on any key is
   * recorded and skipped; progress is never blocked because the DB commit remains authoritative.
   * Successfully acquired keys are appended to {@code heldLocks} in acquisition order.
   */
  private void acquireLocks(
      @Nonnull final Iterable<ConflictKey> conflictKeys, @Nonnull final List<String> heldLocks) {
    final CoordinationLockProvider provider = requireProvider();
    final long acquireWaitMs = config.getLockAcquireTimeoutSeconds() * 1000L;
    final long leaseMs = config.getLockLeaseSeconds() * 1000L;
    // Single deadline for the whole loop so the total acquire wait is bounded regardless of the
    // number of conflict keys -- a large plan can never stall the consumer for (#keys x
    // acquireWaitMs). System.currentTimeMillis is fine here: this is production wall-clock
    // budgeting,
    // not a monotonic-duration measurement.
    final long deadlineMs = System.currentTimeMillis() + acquireWaitMs;

    // conflictKeys is a SortedSet of distinct ConflictKeys and lockKey is injective, so the
    // composed
    // keys are already unique -- no dedup scan needed (acquisition stays O(n)).
    for (final ConflictKey conflictKey : conflictKeys) {
      final String key = lockKey(conflictKey);
      final long remainingMs = deadlineMs - System.currentTimeMillis();
      if (remainingMs <= 0) {
        // Shared deadline exhausted: skip the remaining keys without waiting. Safe — DB commit is
        // authoritative.
        recordLockTimeout();
        log.debug("Lock-acquire deadline reached before {}; proceeding best-effort.", key);
        continue;
      }
      if (provider.tryLock(key, remainingMs, leaseMs)) {
        heldLocks.add(key);
      } else {
        // Bounded wait elapsed (or interrupt): not fully coordinated for this key. Safe — DB commit
        // is authoritative.
        recordLockTimeout();
        log.debug("Timed out acquiring coordination lock {}; proceeding best-effort.", key);
      }
    }
  }

  /**
   * Releases held locks in reverse acquisition order via the provider, which tolerates an
   * already-released or expired lease.
   */
  private void releaseLocks(@Nonnull final List<String> heldLocks) {
    final CoordinationLockProvider provider = requireProvider();
    for (int i = heldLocks.size() - 1; i >= 0; i--) {
      provider.unlock(heldLocks.get(i));
    }
  }

  @Nonnull
  private CoordinationLockProvider requireProvider() {
    // Only reached from the locking path, which is guarded by a null check in execute().
    if (lockProvider == null) {
      throw new IllegalStateException("Lock provider must be present on the coordination path.");
    }
    return lockProvider;
  }

  @Nonnull
  private static String lockKey(@Nonnull final ConflictKey conflictKey) {
    // NUL separator: ids are URNs and can contain '/', ':', ',', etc.; a control char that cannot
    // appear in a URN or a domain keeps the composed key injective, so two distinct conflict keys
    // can
    // never collide onto a single lock (e.g. domain "a"/id "b/c" vs domain "a/b"/id "c").
    return conflictKey.domain() + '\u0000' + conflictKey.id();
  }

  private void recordLockTimeout() {
    if (metricUtils != null) {
      metricUtils.incrementMicrometer(METRIC_LOCK_TIMEOUTS, 1.0d);
    }
  }

  /**
   * Emits {@link #METRIC_LEASE_EXCEEDED} and a warning when the commit outran the IMap lease. No-op
   * when no lock was held (nothing to expire) -- the DB sorted commit is authoritative either way.
   */
  private void recordLeaseExceeded(final long commitStartMs, final boolean noLocksHeld) {
    if (noLocksHeld) {
      return;
    }
    final long elapsedMs = System.currentTimeMillis() - commitStartMs;
    final long leaseMs = config.getLockLeaseSeconds() * 1000L;
    if (elapsedMs > leaseMs) {
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(METRIC_LEASE_EXCEEDED, 1.0d);
      }
      log.warn(
          "Coordinated commit ran {}ms, exceeding the {}ms lock lease; the lease may have expired "
              + "mid-commit and a second writer could have entered (DB sorted commit remains "
              + "authoritative).",
          elapsedMs,
          leaseMs);
    }
  }

  private void recordPlanConflictKeys(final int count) {
    if (metricUtils != null) {
      // Distribution of per-plan conflict-key counts (not a running total).
      metricUtils.recordDistribution(METRIC_PLAN_CONFLICT_KEYS, count);
    }
  }
}
