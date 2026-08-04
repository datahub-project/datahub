package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import javax.annotation.Nonnull;

/**
 * {@link RetentionBuffer} adapter over a {@link CoalesceBuffer}. Keeps the retention-domain API
 * (urn/aspect, keep-max version) separate from the buffer's Hazelcast backend so retention callers
 * never see {@code IMap} types.
 */
public class CoalesceRetentionBuffer implements RetentionBuffer {

  private final CoalesceBuffer<RetentionKey, Long> buffer;

  public CoalesceRetentionBuffer(@Nonnull CoalesceBuffer<RetentionKey, Long> buffer) {
    this.buffer = buffer;
  }

  @Override
  public void enqueue(@Nonnull Urn urn, @Nonnull String aspectName, long maxVersionHint) {
    buffer.merge(
        new RetentionKey(urn.toString(), aspectName),
        maxVersionHint,
        CoalesceBuffers.KEEP_MAX_LONG);
  }

  @Override
  public boolean defersApply() {
    return true;
  }
}
