package com.linkedin.metadata.entity.coordinator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Distributed {@link CoordinationLockProvider} backed by a Hazelcast {@link IMap}. Uses the map's
 * per-key fenced lock ({@link IMap#tryLock}/{@link IMap#unlock}) with an auto-expiry lease so a
 * dead holder cannot wedge a conflict key across the cluster.
 *
 * <p>Best-effort only: a missed acquire or an expired lease never blocks progress — the coordinated
 * DB commit remains authoritative.
 */
@Slf4j
public class HazelcastLockProvider implements CoordinationLockProvider {

  private static final String NAME = "hazelcast";

  /** Name of the {@link IMap} used purely for per-conflict-key locking. */
  public static final String LOCK_MAP_NAME = "datahub-coordinated-ingest-locks";

  @Nonnull private final IMap<String, Object> lockMap;

  /**
   * @param hazelcast the Hazelcast instance providing the lock {@link IMap}
   */
  public HazelcastLockProvider(@Nonnull final HazelcastInstance hazelcast) {
    this.lockMap = hazelcast.getMap(LOCK_MAP_NAME);
  }

  @Override
  public boolean tryLock(@Nonnull final String key, final long waitMillis, final long leaseMillis) {
    try {
      return lockMap.tryLock(key, waitMillis, MILLISECONDS, leaseMillis, MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted acquiring coordination lock {}; proceeding uncoordinated.", key);
      return false;
    }
  }

  @Override
  public void unlock(@Nonnull final String key) {
    try {
      lockMap.unlock(key);
    } catch (final IllegalMonitorStateException e) {
      // Lease expired or the lock was lost/never held — benign for a best-effort serializer.
      log.debug("Coordination lock {} already released or lease expired.", key);
    } catch (final RuntimeException e) {
      log.warn("Failed to release coordination lock {}.", key, e);
    }
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public String name() {
    return NAME;
  }
}
