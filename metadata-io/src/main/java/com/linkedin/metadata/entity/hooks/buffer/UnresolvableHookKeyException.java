package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.metadata.buffer.offload.UnresolvableOffloadKeyException;

/**
 * Thrown by a {@link HookContextResolver} when a drained {@link HookKey} will <em>never</em>
 * resolve (e.g. a subtype the resolver does not produce — a wiring bug or a stale rolling-deploy
 * entry). The {@link PostCommitHookDrainer} (via the generic {@link
 * com.linkedin.metadata.buffer.offload.OffloadDrainer}) drops such keys from the buffer so they
 * don't re-throw every tick. Any other {@link RuntimeException} from the resolver is treated as
 * transient and the key is left for retry.
 *
 * <p>Extends {@link UnresolvableOffloadKeyException} so the framework drainer catches it uniformly
 * without knowing the hook-specific subtype. Mirrors {@code UnresolvableRetentionKeyException}.
 */
public class UnresolvableHookKeyException extends UnresolvableOffloadKeyException {
  public UnresolvableHookKeyException(String message) {
    super(message);
  }

  public UnresolvableHookKeyException(String message, Throwable cause) {
    super(message, cause);
  }
}
