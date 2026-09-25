package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.RetentionKey;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * {@link RetentionBuffer} adapter over a {@link CoalesceBuffer}. Keeps the retention-domain API
 * (urn/aspect, keep-max version) separate from the buffer's Hazelcast backend so retention callers
 * never see {@code IMap} types.
 *
 * <p>Enqueue routes through a {@link RetentionContextResolver} so the buffer key carries whatever
 * routing metadata the resolver attaches from the request {@link OperationContext}. The drainer
 * uses the same resolver to reconstruct a per-group context at drain time.
 */
public class CoalesceRetentionBuffer implements RetentionBuffer {

  private final CoalesceBuffer<RetentionKey, Long> buffer;
  private final RetentionContextResolver contextResolver;

  public CoalesceRetentionBuffer(
      @Nonnull CoalesceBuffer<RetentionKey, Long> buffer,
      @Nonnull RetentionContextResolver contextResolver) {
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
    buffer.merge(key, maxVersionHint, CoalesceBuffers.KEEP_MAX_LONG);
  }

  @Override
  public boolean defersApply() {
    return true;
  }
}
