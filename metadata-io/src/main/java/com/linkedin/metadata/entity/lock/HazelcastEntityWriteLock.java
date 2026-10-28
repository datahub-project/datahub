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
 * Hazelcast-backed write gate: a distributed per-key mutex over a shared {@code IMap<String,
 * Boolean>}. Keys are opaque strings supplied by the caller (the {@code (urn, aspect)} conflict
 * unit), so concurrent writers on the same key queue here instead of thrashing CAS, and they wait
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

  @Override
  public boolean isActive() {
    return true;
  }

  @Nonnull
  @Override
  public LockHandle acquire(@Nonnull OperationContext opContext, @Nonnull Collection<String> urns) {
    // Sorted + de-duplicated acquisition order → no ABBA hold-and-wait between overlapping batches.
    final List<String> sorted = urns.stream().distinct().sorted().collect(Collectors.toList());
    final List<String> acquired = new ArrayList<>(sorted.size());
    // ONE acquisition deadline for the whole batch: total wait is bounded by acquireTimeoutSeconds,
    // NOT acquireTimeoutSeconds * urns.size(). Each URN gets whatever budget remains; once the
    // deadline passes, remaining URNs get a non-blocking tryLock and degrade lockless.
    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(acquireTimeoutSeconds);
    // Set when the loop stops early (interrupt/outage). URNs left unattempted after an early break
    // are NOT deadline misses, so the aggregate "half missed" WARN below must not fire on top of
    // the
    // specific acquire_interrupted/acquire_error warning (which already explains what happened).
    boolean stoppedEarly = false;
    for (String urn : sorted) {
      final long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
      boolean ok = false;
      try {
        ok =
            lockMap.tryLock(
                urn, remainingNanos, TimeUnit.NANOSECONDS, leaseSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // Interrupt is external cancellation (shutdown / request timeout), NOT a lock failure.
        // Preserve the flag so the cancellation propagates — the in-flight write may then be
        // aborted downstream by it. Stop acquiring; keys already taken still release via the
        // handle.
        Thread.currentThread().interrupt();
        incMetric(opContext, "acquire_interrupted");
        log.warn(
            "Interrupted acquiring entity write-lock for urn={}; interrupt preserved, stopping.",
            urn);
        stoppedEarly = true;
        break;
      } catch (RuntimeException e) {
        // Hazelcast unavailable / partitioned: degrade to lockless and STOP for this batch.
        // Retrying every remaining URN would issue a failing remote op + stack trace each,
        // amplifying latency and log volume during an outage. Already-acquired keys still release.
        incMetric(opContext, "acquire_error");
        log.warn(
            "Entity write-lock acquire failed for urn={}; proceeding lockless for the rest of this "
                + "batch (CAS guards).",
            urn,
            e);
        stoppedEarly = true;
        break;
      }
      if (ok) {
        acquired.add(urn);
      } else {
        incMetric(opContext, "acquire_timeout");
        log.debug(
            "Entity write-lock not acquired for urn={} within the batch deadline; proceeding "
                + "lockless.",
            urn);
      }
    }
    // A single contended URN missing the gate is normal (best-effort degrade), so single-URN
    // acquires (e.g. the delete path) never warn here. But if a large fraction of a real BATCH
    // misses, that usually signals a misconfig (acquire timeout too low, or Hazelcast
    // overloaded/partitioned) rather than genuine contention — surface it once per batch at WARN so
    // it isn't invisible behind the per-URN debug lines.
    final int missed = sorted.size() - acquired.size();
    if (!stoppedEarly && sorted.size() > 1 && missed * 2 >= sorted.size()) {
      log.warn(
          "Entity write-gate acquired only {}/{} URNs (>= half missed) within {}s; those writers "
              + "proceed lockless (CAS still guards). Check ENTITY_WRITE_LOCK_ACQUIRE_TIMEOUT_SECONDS "
              + "and Hazelcast health.",
          acquired.size(),
          sorted.size(),
          acquireTimeoutSeconds);
    }
    // One-shot handle: IMap locks are reentrant per (thread, key), so a double close() would unlock
    // a LATER re-acquisition of the same URN on this thread and let a concurrent writer through the
    // gate. Release exactly once. (acquire + close run on the same thread — no synchronization
    // needed.)
    return new LockHandle() {
      private boolean closed = false;

      @Override
      public void close() {
        if (closed) {
          return;
        }
        closed = true;
        release(opContext, acquired);
      }
    };
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
