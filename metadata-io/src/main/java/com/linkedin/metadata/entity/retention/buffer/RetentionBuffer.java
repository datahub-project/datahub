package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * Coalescing buffer for post-commit retention requests. Implementations collapse repeated requests
 * for the same (urn, aspect) into a single "keep max version" entry so a background drainer can
 * apply retention once instead of once per upsert.
 */
public interface RetentionBuffer {

  /**
   * Enqueue a retention request for the given urn/aspect, keeping the highest {@code
   * maxVersionHint} seen so far for that key.
   */
  void enqueue(@Nonnull Urn urn, @Nonnull String aspectName, long maxVersionHint);

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
        public void enqueue(Urn urn, String aspectName, long maxVersionHint) {}

        @Override
        public boolean defersApply() {
          return false;
        }
      };
}
