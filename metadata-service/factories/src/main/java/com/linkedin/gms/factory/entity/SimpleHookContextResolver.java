package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.hooks.buffer.HookContextResolver;
import com.linkedin.metadata.entity.hooks.buffer.HookKey;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;

/**
 * Default {@link HookContextResolver} for the single-database deployment. Keys carry no routing
 * metadata ({@link SimpleHookKey}); {@link #groupKey} groups by hook id so each hook's pending MCLs
 * replay in their own group under that hook; {@link #resolveOpContext} returns the {@code
 * systemOperationContext} unchanged. The background {@link PostCommitHookDrainer} then replays
 * committed MCLs through their hooks under the system context, which carries a full {@link
 * io.datahubproject.metadata.context.RetrieverContext} (aspect / graph / search retrievers) wired
 * by {@code SystemOperationContextFactory}.
 *
 * <p><b>Grouping by hook id is required.</b> {@code HookDrainAction} replays a whole group through
 * the single hook named by the group's first key and then clears every entry in that group. If two
 * different hooks shared a group, the first hook would run on the second hook's MCLs (filtered out
 * by {@code shouldApply}) and the second hook's entries would be cleared without ever being
 * replayed — silently dropping their side effects.
 *
 * <p>Registered by {@link PostCommitHookBufferFactory#hookContextResolver} under a
 * {@code @ConditionalOnMissingBean(HookContextResolver.class)} guard, so an extension that supplies
 * its own {@link HookContextResolver} bean (keys with routing metadata, grouping by hook + route,
 * reconstructing a per-route {@link OperationContext}) replaces this default without a conflict. An
 * extension's {@code groupKey} MUST include the hook id (plus any route id) to preserve the
 * one-hook-per-group invariant.
 */
public class SimpleHookContextResolver implements HookContextResolver {

  private final OperationContext systemOperationContext;

  public SimpleHookContextResolver(
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  @Nonnull
  public HookKey enrichKey(
      @Nonnull OperationContext opContext,
      @Nonnull String hookId,
      @Nonnull MetadataChangeLog mcl,
      long sequence) {
    return HookContextResolver.simpleKey(hookId, mcl, sequence);
  }

  @Override
  @Nonnull
  public String groupKey(@Nonnull HookKey key) {
    return key.getHookId();
  }

  @Override
  @Nonnull
  public OperationContext resolveOpContext(
      @Nonnull HookKey key, @Nonnull OperationContext systemOperationContext) {
    return systemOperationContext;
  }
}
