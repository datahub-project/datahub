package com.linkedin.metadata.entity.retention;

/**
 * Thrown by a {@link RetentionContextResolver} when a drained {@link RetentionKey} is
 * <em>permanently</em> unresolvable — typically a key subtype the resolver does not produce (a
 * wiring bug, or a stale entry left in the buffer across a rolling deploy that changed the key
 * type). The drainer treats this as fatal for the offending key: it drops the key from the buffer
 * via {@code removeIfSame} so it doesn't re-throw every tick (infinite retry storm).
 *
 * <p>This is distinct from a <em>transient</em> resolver failure (e.g. a temporary lookup error).
 * Transient failures surface as other {@link RuntimeException} subtypes; the drainer leaves those
 * keys queued for retry on the next tick rather than dropping them, so a transient blip cannot
 * silently skip retention.
 *
 * <p>The OSS default {@code SimpleRetentionContextResolver} never throws this — its keys are always
 * resolvable. An extension resolver throws this only when it can prove the key will never resolve
 * (e.g. a subtype mismatch).
 */
public class UnresolvableRetentionKeyException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public UnresolvableRetentionKeyException(@javax.annotation.Nonnull String message) {
    super(message);
  }
}
