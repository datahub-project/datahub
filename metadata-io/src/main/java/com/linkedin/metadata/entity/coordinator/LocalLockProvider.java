package com.linkedin.metadata.entity.coordinator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * In-JVM {@link CoordinationLockProvider} backed by a bounded {@link Striped} of reentrant locks.
 * Conflict keys are hashed to a fixed number of stripes, so memory is bounded regardless of key
 * cardinality; two distinct keys may share a stripe (false sharing), which is acceptable for a
 * best-effort serializer.
 *
 * <p>Uses {@link Striped#lock(int)} (strong, eagerly allocated) rather than {@code lazyWeakLock}:
 * the coordinator holds a lock across the whole commit without retaining a strong reference to it,
 * so a weakly-referenced lock could be collected mid-hold and silently break mutual exclusion. The
 * fixed stripe count keeps the strong allocation bounded.
 *
 * <p><b>Single-JVM only.</b> This provider serializes contending commands within one process. On a
 * multi-node deployment it does <b>not</b> coordinate across nodes — cross-node serialization then
 * relies entirely on the authoritative DB single-sorted {@code FOR UPDATE} commit. Prefer a
 * distributed provider (Hazelcast/Redis) when running more than one GMS node.
 *
 * <p>{@code leaseMillis} is ignored: an in-JVM lock has no dead-holder risk because {@link
 * MutationCoordinator} always releases in a {@code finally}.
 */
@Slf4j
public class LocalLockProvider implements CoordinationLockProvider {

  private static final String NAME = "local";

  /** Bounded stripe count — caps memory while keeping cross-key contention low. */
  private static final int DEFAULT_STRIPES = 256;

  @Nonnull private final Striped<Lock> stripes;

  /** Reported {@link #name()}; distinguishes plain local from a degraded-distributed fallback. */
  @Nonnull private final String name;

  public LocalLockProvider() {
    this(DEFAULT_STRIPES, NAME);
  }

  /**
   * @param stripeCount number of lock stripes; higher reduces false sharing at a small memory cost
   */
  public LocalLockProvider(final int stripeCount) {
    this(stripeCount, NAME);
  }

  /**
   * @param name reported name — lets a degraded distributed provider surface itself in metrics/logs
   *     (e.g. {@code hazelcast-degraded}) rather than masquerading as plain {@code local}
   */
  public LocalLockProvider(@Nonnull final String name) {
    this(DEFAULT_STRIPES, name);
  }

  public LocalLockProvider(final int stripeCount, @Nonnull final String name) {
    this.stripes = Striped.lock(stripeCount);
    this.name = name;
  }

  @Override
  public boolean tryLock(@Nonnull final String key, final long waitMillis, final long leaseMillis) {
    final Lock lock = stripes.get(key);
    try {
      return lock.tryLock(waitMillis, MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted acquiring local coordination lock {}; proceeding uncoordinated.", key);
      return false;
    }
  }

  @Override
  public void unlock(@Nonnull final String key) {
    final Lock lock = stripes.get(key);
    try {
      lock.unlock();
    } catch (final IllegalMonitorStateException e) {
      // Not held by this thread (already released) — benign for a best-effort serializer.
      log.debug("Local coordination lock {} not held; nothing to release.", key);
    }
  }

  @Override
  public boolean isDistributed() {
    return false;
  }

  @Override
  public String name() {
    return name;
  }
}
