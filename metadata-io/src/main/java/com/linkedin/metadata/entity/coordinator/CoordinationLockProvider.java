package com.linkedin.metadata.entity.coordinator;

import javax.annotation.Nonnull;

/**
 * Pluggable best-effort lock substrate for the coordinated-ingest COORDINATE stage. Implementations
 * serialize commands that contend on the same conflict key so the authoritative DB single-sorted
 * {@code FOR UPDATE} commit sees less contention; they are <b>never</b> the correctness floor.
 * Correctness must not depend on any provider — a provider may time out, lose a lease, or (for
 * in-JVM providers) cover only a single node, and the coordinated commit still runs and stays safe
 * via the DB sorted lock.
 *
 * <p>Selected by environment/config (see {@code CoordinatedIngestConfiguration.getLockProvider()})
 * so Hazelcast can be swapped for a future Redis provider or a local in-JVM lock without touching
 * {@link MutationCoordinator}.
 *
 * <p>Implementations must be thread-safe: a single instance is shared across all coordinating
 * threads.
 */
public interface CoordinationLockProvider {

  /**
   * Best-effort acquire of {@code key}, blocking up to {@code waitMillis} for it.
   *
   * @param key the conflict-key lock identifier
   * @param waitMillis maximum time to wait for the lock before giving up
   * @param leaseMillis auto-expiry TTL applied by distributed providers so a dead holder cannot
   *     wedge the key forever; in-JVM providers may ignore it since the coordinator always releases
   *     in a {@code finally}
   * @return {@code true} if the lock was acquired within {@code waitMillis}, {@code false}
   *     otherwise (timeout or interrupt) — a {@code false} return is not an error, the caller
   *     proceeds uncoordinated
   */
  boolean tryLock(@Nonnull String key, long waitMillis, long leaseMillis);

  /**
   * Releases {@code key}. Must tolerate a lock that was already released, expired, or lost (e.g. a
   * distributed lease that timed out): such cases are a no-op and must not throw.
   *
   * @param key the conflict-key lock identifier previously passed to {@link #tryLock}
   */
  void unlock(@Nonnull String key);

  /**
   * Whether this provider coordinates across JVMs/nodes. {@code false} for in-JVM providers,
   * meaning cross-node coordination is <b>not</b> covered and only the DB layer serializes across
   * nodes — surfaced purely as a telemetry/ops signal.
   *
   * @return {@code true} for cross-node providers (e.g. Hazelcast, Redis), {@code false} for in-JVM
   */
  boolean isDistributed();

  /**
   * Short, stable identifier for metrics and logging.
   *
   * @return provider name, e.g. {@code "hazelcast"} or {@code "local"}
   */
  String name();
}
