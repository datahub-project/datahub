package com.linkedin.metadata.buffer;

import javax.annotation.Nonnull;

/**
 * Creates named {@link CoalesceBuffer} instances backed by whichever implementation {@code
 * datahub.buffer.implementation} selects (see {@link BufferImplementation}). Callers depend only on
 * this interface and {@link CoalesceBuffer}, never on the concrete backend.
 */
public interface CoalesceBufferFactory {

  /**
   * @param name identifies the buffer's backing store (e.g. a Hazelcast map name) and labels
   *     overflow metrics; must be unique per logical buffer.
   * @param lockName identifies the backing lock namespace for distributed implementations (e.g. a
   *     second Hazelcast map name for the drain lock); ignored by local-only implementations.
   * @param maxPendingEntries soft cap on distinct pending keys before new keys are dropped
   *     (existing keys can always be updated, never dropped).
   */
  @Nonnull
  <K, V> CoalesceBuffer<K, V> create(
      @Nonnull String name, @Nonnull String lockName, int maxPendingEntries);
}
