package com.linkedin.metadata.buffer;

import javax.annotation.Nonnull;

/**
 * Creates named {@link CoalesceBuffer} instances over the shared embedded Hazelcast instance.
 * Callers depend only on this interface and {@link CoalesceBuffer}, never on the concrete backend.
 */
public interface CoalesceBufferFactory {

  /**
   * @param name identifies the buffer's backing Hazelcast map and labels overflow metrics; must be
   *     unique per logical buffer.
   * @param lockName identifies the Hazelcast map backing the cluster-wide drain lock.
   * @param maxPendingEntries soft cap on distinct pending keys, enforced by the map's eviction
   *     config (existing keys can always be updated, never dropped).
   */
  @Nonnull
  <K, V> CoalesceBuffer<K, V> create(
      @Nonnull String name, @Nonnull String lockName, int maxPendingEntries);
}
