package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.offload.HazelcastOffloadBuffer;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * {@link RetentionBuffer} adapter over the framework {@link OffloadBuffer}. Keeps the
 * retention-domain API (urn/aspect, keep-max version) separate from the buffer's Hazelcast backend
 * so retention callers never see {@code IMap} types.
 *
 * <p>Enqueue routes through a {@link RetentionContextResolver} so the buffer key carries whatever
 * routing metadata the resolver attaches from the request {@link OperationContext}. The drainer
 * uses the same resolver to reconstruct a per-group context at drain time.
 *
 * <p>All infra (drain lock, paging drain, CAS clear, keep-max merge, eviction) lives in the
 * framework {@link HazelcastOffloadBuffer}; this class adds only the retention-specific
 * key/value binding and the drain comparator (value-then-key, matching the historical retention
 * drain order).
 */
public class CoalesceRetentionBuffer implements RetentionBuffer {

  private final OffloadBuffer<RetentionKey, Long> buffer;
  private final RetentionContextResolver<RetentionKey> contextResolver;

  public CoalesceRetentionBuffer(
      @Nonnull OffloadBuffer<RetentionKey, Long> buffer,
      @Nonnull RetentionContextResolver<RetentionKey> contextResolver) {
    this.buffer = buffer;
    this.contextResolver = contextResolver;
  }

  @Override
  public void enqueue(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long maxVersionHint) {
    RetentionKey key = contextResolver.enrichKey(opContext, urn, aspectName);
    buffer.enqueue(key, maxVersionHint);
  }

  @Override
  public boolean defersApply() {
    return true;
  }

  /**
   * The framework drain comparator for the retention buffer: by pending value (max version) then
   * key string, so paging never relies on the key being {@link Comparable}. Order is arbitrary
   * (drain is best-effort), only stability/totality matter. Must be {@link Serializable} (it ships
   * to Hazelcast cluster members for {@code PagingPredicate}).
   */
  @Nonnull
  public static Comparator<Map.Entry<RetentionKey, Long>> drainOrder() {
    return new RetentionDrainOrder();
  }

  /** Serializable value-then-key total order for {@link OffloadBuffer#drain}'s paging. */
  public static final class RetentionDrainOrder
      implements Comparator<Map.Entry<RetentionKey, Long>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Map.Entry<RetentionKey, Long> a, Map.Entry<RetentionKey, Long> b) {
      int byValue = Long.compare(a.getValue(), b.getValue());
      if (byValue != 0) {
        return byValue;
      }
      return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
    }
  }
}
