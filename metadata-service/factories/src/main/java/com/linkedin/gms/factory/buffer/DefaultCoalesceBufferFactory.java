package com.linkedin.gms.factory.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.buffer.BufferImplementation;
import com.linkedin.metadata.buffer.CaffeineCoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBufferFactory;
import com.linkedin.metadata.buffer.HazelcastCoalesceBuffer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds {@link CoalesceBuffer}s backed by whichever implementation was resolved from {@code
 * datahub.buffer.implementation} at construction time. Falls back to {@link
 * BufferImplementation#CAFFEINE} if Hazelcast was selected but no {@link HazelcastInstance} bean is
 * available (e.g. flag mismatch), so callers never see a broken factory.
 */
@Slf4j
public class DefaultCoalesceBufferFactory implements CoalesceBufferFactory {

  private final BufferImplementation implementation;
  @Nullable private final HazelcastInstance hazelcastInstance;
  @Nullable private final MetricUtils metricUtils;

  public DefaultCoalesceBufferFactory(
      @Nonnull BufferImplementation implementation,
      @Nullable HazelcastInstance hazelcastInstance,
      @Nullable MetricUtils metricUtils) {
    if (implementation == BufferImplementation.HAZELCAST && hazelcastInstance == null) {
      log.warn(
          "datahub.buffer.implementation=hazelcast but no Hazelcast instance is available;"
              + " falling back to caffeine (local-only coalescing)");
      this.implementation = BufferImplementation.CAFFEINE;
    } else {
      this.implementation = implementation;
    }
    this.hazelcastInstance = hazelcastInstance;
    this.metricUtils = metricUtils;
  }

  @Override
  @Nonnull
  @SuppressWarnings("unchecked")
  public <K, V> CoalesceBuffer<K, V> create(
      @Nonnull String name, @Nonnull String lockName, int maxPendingEntries) {
    if (implementation == BufferImplementation.HAZELCAST) {
      // Hazelcast backend is Long-valued only (KEEP_MAX_LONG EntryProcessor). The unchecked cast
      // below can't be validated at wiring time (V is erased), but a V != Long caller can only pass
      // a BinaryOperator<V> that isn't KEEP_MAX_LONG, which HazelcastCoalesceBuffer.merge rejects
      // with UnsupportedOperationException on first use — a clear fail, not a silent CCE. Callers
      // needing other V must use caffeine, or add new processors here.
      // maxPendingEntries is enforced by MapConfig (RetentionBufferFactory), not on the hot
      // merge path — avoids distributed size()/containsKey() on every ingest enqueue.
      return (CoalesceBuffer<K, V>)
          (CoalesceBuffer<?, ?>) new HazelcastCoalesceBuffer<K>(hazelcastInstance, name, lockName);
    }
    return new CaffeineCoalesceBuffer<>(name, maxPendingEntries, metricUtils);
  }
}
