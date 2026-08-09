package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Coalescing buffer for post-commit retention requests. Implementations collapse repeated requests
 * for the same key into a single "keep max version" entry so a background drainer can apply
 * retention once instead of once per upsert.
 *
 * <p>{@code enqueue} takes the request {@link OperationContext} so the buffer can attach routing
 * metadata to the key (via a {@link RetentionContextResolver}) and the drainer can later route the
 * drained entry to the same underlying database that produced it.
 */
public interface RetentionBuffer {

  /**
   * Enqueue a retention request for the given urn/aspect, keeping the highest {@code
   * maxVersionHint} seen so far for that key. Routing metadata is attached from {@code opContext}
   * by the buffer's {@link RetentionContextResolver}.
   */
  void enqueue(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long maxVersionHint);

  /**
   * @return true if callers must NOT apply retention synchronously; a background drainer will apply
   *     it instead. False means there is no buffer backing this instance and callers should fall
   *     back to synchronous retention.
   */
  boolean defersApply();

  /** No-op buffer (default): retention runs synchronously whenever coalescing is not wired. */
  RetentionBuffer NO_OP =
      new RetentionBuffer() {
        @Override
        public void enqueue(
            OperationContext opContext, Urn urn, String aspectName, long maxVersionHint) {}

        @Override
        public boolean defersApply() {
          return false;
        }
      };
}
