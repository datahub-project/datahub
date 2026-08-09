package com.linkedin.metadata.entity.retention;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.offload.OffloadContextResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * Seam for attaching routing metadata to retention buffer keys and reconstructing an {@link
 * OperationContext} for a drained key, so a background drainer can route each drained entry to the
 * same underlying database that produced it.
 *
 * <p>Extends the framework {@link OffloadContextResolver} drain-side seam ({@link #groupKey} /
 * {@link #resolveOpContext}) and adds the enqueue-side {@link #enrichKey}: the drainer has no
 * request context of its own (it runs on a scheduler thread), so without this seam every drained
 * entry would be applied under a single system context and route to one fixed database, silently
 * missing entries that originated against a different one. The resolver captures enough routing
 * metadata at enqueue (when the request {@link OperationContext} is available) to reconstruct a
 * correct context at drain.
 *
 * <p>OSS default {@code SimpleRetentionContextResolver} is a no-op: keys carry no routing metadata
 * and {@code resolveOpContext} returns the system context unchanged, matching the single-database
 * deployment. An extension module that routes to multiple databases provides its own implementation
 * (and a matching {@link RetentionKey} subtype).
 *
 * <p><b>Failure contract.</b> {@link #groupKey} and {@link #resolveOpContext} distinguish permanent
 * from transient failures: throw {@link UnresolvableRetentionKeyException} for a key that will
 * <em>never</em> resolve (e.g. a subtype the resolver does not produce — a wiring bug or a stale
 * rolling-deploy entry). The framework {@code OffloadDrainer} drops such keys from the buffer so
 * they don't re-throw every tick. Any other {@link RuntimeException} is treated as transient: when
 * the drainer's backoff is enabled it moves the key to a backoff limbo (re-merged after
 * {@code backoffTicks}); otherwise it leaves the key queued for retry on the next tick, so a
 * transient blip cannot silently skip retention.
 *
 * @param <K> buffer key type (must be {@link Serializable} for Hazelcast)
 */
public interface RetentionContextResolver<K extends Serializable> extends OffloadContextResolver<K> {

  /**
   * Build the buffer key for a retention request, attaching any routing metadata available on
   * {@code opContext}. Called at enqueue, where the request {@link OperationContext} is available.
   */
  @Nonnull
  K enrichKey(@Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName);
}
