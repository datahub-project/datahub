package com.linkedin.metadata.buffer.offload;

import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * Drain-side context seam for an {@link OffloadDrainer}. This is the framework-level half of a use
 * resolver: only the two methods the <em>drainer</em> needs are here. The enqueue-side method
 * ({@code enrichKey}) stays on the use-specific resolver (e.g. {@code HookContextResolver}) because
 * its argument list is use-specific (hooks: {@code opContext, hookId, mcl, sequence}; retention:
 * {@code opContext, urn, aspectName}).
 *
 * <p>The drainer runs on a scheduler thread with no request context of its own. It groups drained
 * entries by {@link #groupKey} so entries sharing a routing context replay together, then rebuilds
 * the correct {@link OperationContext} for that group via {@link #resolveOpContext}. Without this,
 * every replay would run under one fixed system context and route to one database — silently
 * missing entries from other routes, or (multi-tenant cloud) replaying against the wrong tenant's
 * catalog.
 *
 * <p><b>Failure contract.</b> Throw {@link UnresolvableOffloadKeyException} (or a subtype) for a
 * key that will <em>never</em> resolve (e.g. a subtype the resolver does not produce — a wiring bug
 * or a stale rolling-deploy entry). The drainer drops such keys so they don't re-throw every tick.
 * Any other {@link RuntimeException} is transient: the drainer logs it and leaves the entries
 * queued for the next tick, so a transient blip cannot silently skip a replay.
 *
 * @param <K> buffer key type
 */
public interface OffloadContextResolver<K extends Serializable> {

  /**
   * Stable grouping key for drained entries. The drainer groups drained entries by this so entries
   * sharing a routing context are replayed together under one {@link #resolveOpContext}. Return a
   * value stable across calls for the same routing context (e.g. a tenant id extracted from the
   * key).
   */
  @Nonnull
  String groupKey(@Nonnull K key);

  /**
   * Reconstruct the {@link OperationContext} to replay work for a drained group. Called once per
   * group; the returned context is used for every entry in that group.
   *
   * @param key any key in the group (routing metadata is read from it)
   * @param systemOperationContext the bootstrap system context to base the result on
   */
  @Nonnull
  OperationContext resolveOpContext(
      @Nonnull K key, @Nonnull OperationContext systemOperationContext);
}
