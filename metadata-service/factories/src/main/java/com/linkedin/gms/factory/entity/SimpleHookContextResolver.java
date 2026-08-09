package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.hooks.buffer.HookContextResolver;
import com.linkedin.metadata.entity.hooks.buffer.HookKey;
import com.linkedin.metadata.entity.hooks.buffer.SimpleHookKey;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;

/**
 * OSS default {@link HookContextResolver} for the single-database (single-tenant) deployment.
 * Keys carry no routing metadata ({@link SimpleHookKey}); {@link #groupKey} returns a single
 * constant so all drained replays run under one group; {@link #resolveOpContext} returns the
 * {@code systemOperationContext} unchanged. The background {@link
 * PostCommitHookDrainer} then replays committed MCLs through their hooks under the system context,
 * which carries a full {@link io.datahubproject.metadata.context.RetrieverContext} (aspect /
 * graph / search retrievers) wired by {@code SystemOperationContextFactory}.
 *
 * <p>Registered by {@link PostCommitHookBufferFactory#hookContextResolver} under a
 * {@code @ConditionalOnMissingBean(HookContextResolver.class)} guard, so a cloud multi-tenant
 * extension that supplies its own {@link HookContextResolver} bean (carrying {@code tenantId} on
 * the key, grouping by tenant, reconstructing a per-tenant {@link OperationContext} via {@code
 * TenantContexts.withTenant}) replaces this default without a conflict.
 */
public class SimpleHookContextResolver implements HookContextResolver {

  /** Single group for the single-database deployment; all drained replays share one context. */
  static final String SINGLE_GROUP = "default";

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
    String urn = mcl.getEntityUrn() == null ? "" : mcl.getEntityUrn().toString();
    String aspectName = mcl.getAspectName() == null ? "" : mcl.getAspectName();
    return new SimpleHookKey(hookId, urn, aspectName, sequence);
  }

  @Override
  @Nonnull
  public String groupKey(@Nonnull HookKey key) {
    return SINGLE_GROUP;
  }

  @Override
  @Nonnull
  public OperationContext resolveOpContext(
      @Nonnull HookKey key, @Nonnull OperationContext systemOperationContext) {
    return systemOperationContext;
  }
}
