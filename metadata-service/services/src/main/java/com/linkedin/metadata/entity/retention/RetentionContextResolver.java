package com.linkedin.metadata.entity.retention;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Seam for attaching routing metadata to retention buffer keys and reconstructing an {@link
 * OperationContext} for a drained key, so a background drainer can route each drained entry to the
 * same underlying database that produced it.
 *
 * <p>The drainer has no request context of its own — it runs on a scheduler thread. Without this
 * seam, every drained entry would be applied under a single system context and route to one fixed
 * database, silently missing entries that originated against a different one. The resolver captures
 * enough routing metadata at enqueue (when the request {@link OperationContext} is available) to
 * reconstruct a correct context at drain.
 *
 * <p>OSS default {@code SimpleRetentionContextResolver} is a no-op: keys carry no routing metadata
 * and {@code resolveOpContext} returns the system context unchanged, matching the single-database
 * deployment. An extension module that routes to multiple databases provides its own implementation
 * (and a matching {@link RetentionKey} subtype).
 *
 * <p><b>Failure contract.</b> {@link #groupKey} and {@link #resolveOpContext} distinguish permanent
 * from transient failures: throw {@link UnresolvableRetentionKeyException} for a key that will
 * <em>never</em> resolve (e.g. a subtype the resolver does not produce — a wiring bug or a stale
 * rolling-deploy entry). The drainer drops such keys from the buffer so they don't re-throw every
 * tick. Any other {@link RuntimeException} is treated as transient: the drainer logs it and leaves
 * the key queued for retry on the next tick, so a transient blip cannot silently skip retention.
 */
public interface RetentionContextResolver {

  /**
   * Build the buffer key for a retention request, attaching any routing metadata available on
   * {@code opContext}. Called at enqueue, where the request {@link OperationContext} is available.
   */
  @Nonnull
  RetentionKey enrichKey(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName);

  /**
   * Stable grouping key for drained entries. The drainer groups drained entries by this so that
   * entries sharing a routing context are applied together in one batch call. Implementations
   * should return a value that is stable across calls for the same routing context (e.g. a routing
   * identifier extracted from the key).
   */
  @Nonnull
  String groupKey(@Nonnull RetentionKey key);

  /**
   * Reconstruct the {@link OperationContext} to apply retention for a drained group. Called once
   * per group (see {@link #groupKey}); the returned context is used for every entry in that group.
   *
   * @param systemOperationContext the bootstrap system context to base the result on
   */
  @Nonnull
  OperationContext resolveOpContext(
      @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext);
}
