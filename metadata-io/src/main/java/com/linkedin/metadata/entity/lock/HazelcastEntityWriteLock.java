package com.linkedin.metadata.entity.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed write gate: a distributed per-URN mutex over a shared {@code IMap<String,
 * Boolean>}. Concurrent writers on the same URN queue here instead of thrashing CAS, and they wait
 * in Hazelcast rather than pinning a pooled DB connection.
 *
 * <ul>
 *   <li><b>Deadlock-free:</b> URNs are locked in sorted order, so two batches that overlap can
 *       never hold-and-wait in opposite orders.
 *   <li><b>Crash-safe:</b> {@code tryLock} takes a lease, so a holder JVM that dies or hangs frees
 *       the URN automatically; a member leaving the cluster also releases its held key-locks. A
 *       stuck writer can never permanently wedge a URN.
 *   <li><b>Best-effort:</b> a URN not lockable within the timeout is skipped and the write proceeds
 *       lockless — CAS on {@code SystemMetadata.version} is the correctness guard, so this only
 *       costs some CAS thrash, never a lost or corrupted write. {@link #acquire} never throws.
 * </ul>
 *
 * <p>Hazelcast IMap locks are thread-owned, so the returned handle MUST be closed on the same
 * thread that called {@link #acquire}. The lock is not fair: it serializes writers (no
 * thundering-herd CAS thrash) but does not guarantee strict submission-order FIFO across the fleet.
 */
@Slf4j
public final class HazelcastEntityWriteLock implements EntityWriteLock {

  private final IMap<String, Boolean> lockMap;
  private final long acquireTimeoutSeconds;
  private final long leaseSeconds;

  public HazelcastEntityWriteLock(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String mapName,
      long acquireTimeoutSeconds,
      long leaseSeconds) {
    this.lockMap = hazelcastInstance.getMap(mapName);
    this.acquireTimeoutSeconds = acquireTimeoutSeconds;
    this.leaseSeconds = leaseSeconds;
  }

  @Nonnull
  @Override
  public LockHandle acquire(@Nonnull OperationContext opContext, @Nonnull Collection<String> urns) {
    // Sorted + de-duplicated acquisition order → no ABBA hold-and-wait between overlapping batches.
    final List<String> sorted = urns.stream().distinct().sorted().collect(Collectors.toList());
    final List<String> acquired = new ArrayList<>(sorted.size());
    for (String urn : sorted) {
      boolean ok = false;
      try {
        ok =
            lockMap.tryLock(
                urn, acquireTimeoutSeconds, TimeUnit.SECONDS, leaseSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // Interrupt is external cancellation (shutdown / request timeout), NOT a lock failure.
        // Preserve the flag so the cancellation propagates — the in-flight write may then be
        // aborted
        // downstream by it, which is the correct response to cancellation. Skip remaining locks.
        Thread.currentThread().interrupt();
        incMetric(opContext, "acquire_interrupted");
        log.warn(
            "Interrupted acquiring entity write-lock for urn={}; interrupt preserved, skipping "
                + "remaining locks.",
            urn);
        break;
      } catch (RuntimeException e) {
        // Hazelcast unavailable / partitioned: degrade to lockless rather than fail the write.
        // Distinct metric from a plain timeout so an outage is not misdiagnosed as contention.
        incMetric(opContext, "acquire_error");
        log.warn(
            "Entity write-lock acquire failed for urn={}; proceeding lockless (CAS guards).",
            urn,
            e);
        continue;
      }
      if (ok) {
        acquired.add(urn);
      } else {
        incMetric(opContext, "acquire_timeout");
        log.debug(
            "Entity write-lock not acquired for urn={} within {}s; proceeding lockless.",
            urn,
            acquireTimeoutSeconds);
      }
    }
    return () -> release(opContext, acquired);
  }

  private void release(@Nonnull OperationContext opContext, @Nonnull List<String> acquired) {
    for (int i = acquired.size() - 1; i >= 0; i--) {
      final String urn = acquired.get(i);
      try {
        lockMap.unlock(urn);
      } catch (IllegalMonitorStateException e) {
        // Benign: lease already expired / lock freed. (Also thrown on a cross-thread close, which
        // the
        // same-thread contract forbids — hence debug, not silent.)
        log.debug("Entity write-lock already released (lease expired?) for urn={}", urn);
      } catch (RuntimeException e) {
        // Genuine release failure: the URN may stay locked until the lease expires. Metric'd so a
        // spike is alertable, unlike the benign case above.
        incMetric(opContext, "release_failed");
        log.warn(
            "Entity write-lock release failed for urn={} (may stay held until lease expiry)",
            urn,
            e);
      }
    }
  }

  private static void incMetric(@Nonnull OperationContext opContext, @Nonnull String name) {
    opContext.getMetricUtils().ifPresent(m -> m.increment(HazelcastEntityWriteLock.class, name, 1));
  }
}
