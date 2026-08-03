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
  static final String METRIC_ACTIVE_CONFLICT_KEYS = "coordinator.active_conflict_keys";

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
    recordActiveConflictKeys(plan.conflictKeys().size());

    if (lockProvider == null) {
      // No lock substrate: no-op locking. The DB single-sorted commit remains authoritative.
      return commit.commitUnderLock(plan);
    }

    final List<String> heldLocks = new ArrayList<>();
    try {
      acquireLocks(plan.conflictKeys(), heldLocks);
      return commit.commitUnderLock(plan);
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

    for (final ConflictKey conflictKey : conflictKeys) {
      final String key = lockKey(conflictKey);
      if (heldLocks.contains(key)) {
        continue;
      }
      if (provider.tryLock(key, acquireWaitMs, leaseMs)) {
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
    return conflictKey.domain() + "/" + conflictKey.id();
  }

  private void recordLockTimeout() {
    if (metricUtils != null) {
      metricUtils.incrementMicrometer(METRIC_LOCK_TIMEOUTS, 1.0d);
    }
  }

  private void recordActiveConflictKeys(final int count) {
    if (metricUtils != null) {
      metricUtils.incrementMicrometer(METRIC_ACTIVE_CONFLICT_KEYS, count);
    }
  }
}
