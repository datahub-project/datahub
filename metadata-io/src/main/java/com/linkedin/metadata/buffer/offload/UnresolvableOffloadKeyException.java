package com.linkedin.metadata.buffer.offload;

/**
 * Thrown by an {@link OffloadContextResolver} when a drained key can <em>never</em> be resolved to a
 * routing context — e.g. a key subtype the resolver does not produce (a wiring bug or a stale entry
 * left by a rolling deploy). The {@link OffloadDrainer} treats this as permanent: it drops the
 * key's entries from the buffer and records a metric, so a poison key does not re-throw every tick.
 *
 * <p>Use-specific subclasses (e.g. {@code UnresolvableHookKeyException}) may extend this so the
 * generic drainer catches them uniformly.
 */
public class UnresolvableOffloadKeyException extends RuntimeException {
  public UnresolvableOffloadKeyException(String message) {
    super(message);
  }

  public UnresolvableOffloadKeyException(String message, Throwable cause) {
    super(message, cause);
  }
}
