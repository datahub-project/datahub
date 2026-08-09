package com.linkedin.metadata.entity.retention;

import com.linkedin.metadata.buffer.offload.UnresolvableOffloadKeyException;
import javax.annotation.Nonnull;

/**
 * Thrown by a {@link RetentionContextResolver} when a drained key can <em>never</em> be resolved
 * (e.g. a key subtype the resolver does not produce — a wiring bug or a stale rolling-deploy entry).
 * The framework {@code OffloadDrainer} drops such keys from the buffer so they don't re-throw every
 * tick; this is a permanent-failure signal, distinct from a transient {@link RuntimeException}
 * (which is retried, optionally via backoff).
 */
public class UnresolvableRetentionKeyException extends UnresolvableOffloadKeyException {

  private static final long serialVersionUID = 1L;

  public UnresolvableRetentionKeyException(@Nonnull String message) {
    super(message);
  }
}
