package com.linkedin.gms.factory.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBufferFactory;
import com.linkedin.metadata.buffer.HazelcastCoalesceBuffer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds {@link HazelcastCoalesceBuffer}s over the shared embedded {@link HazelcastInstance}. The
 * only backend: coalescing needs a cluster-wide shared map + drain lock so exactly one pod drains
 * per tick, which a local map cannot provide. Discovery is Kubernetes-only ({@code
 * CacheConfig.hazelcastInstance}); outside k8s each JVM forms a single-member cluster and drains
 * its own buffer — still safe (idempotent version-range DELETEs), just no cross-pod coalescing.
 */
public class HazelcastCoalesceBufferFactory implements CoalesceBufferFactory {

  @Nullable private final HazelcastInstance hazelcastInstance;
  @Nullable private final MetricUtils metricUtils;

  public HazelcastCoalesceBufferFactory(
      @Nullable HazelcastInstance hazelcastInstance, @Nullable MetricUtils metricUtils) {
    this.hazelcastInstance = hazelcastInstance;
    this.metricUtils = metricUtils;
  }

  @Override
  @Nonnull
  @SuppressWarnings("unchecked")
  public <K, V> CoalesceBuffer<K, V> create(
      @Nonnull String name, @Nonnull String lockName, int maxPendingEntries) {
    // The factory bean is created eagerly and tolerates a null instance (retention buffer off ->
    // no HazelcastInstance bean); create() is only reached with the flag on, which boots the node
    // via HazelcastInstanceBootstrapCondition. Assert here so a wiring mismatch fails loudly rather
    // than NPE-ing deep in a merge.
    Objects.requireNonNull(
        hazelcastInstance,
        "Retention buffer requires a HazelcastInstance but none was provisioned; check"
            + " featureFlags.retentionBufferEnabled and HazelcastInstanceBootstrapCondition");
    // maxPendingEntries is enforced by the bounded MapConfig registered in RetentionBufferFactory
    // (Hazelcast eviction), not per-merge here — see HazelcastCoalesceBuffer "Sizing".
    return (CoalesceBuffer<K, V>)
        (CoalesceBuffer<?, ?>)
            new HazelcastCoalesceBuffer<K>(hazelcastInstance, name, lockName, metricUtils);
  }
}
