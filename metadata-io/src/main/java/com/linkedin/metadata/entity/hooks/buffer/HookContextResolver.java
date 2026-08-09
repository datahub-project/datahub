package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.metadata.buffer.offload.OffloadContextResolver;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Seam for attaching routing metadata to post-commit hook buffer keys and reconstructing an {@link
 * OperationContext} (with a {@link com.linkedin.metadata.aspect.RetrieverContext}) for a drained
 * group, so the background {@link PostCommitHookDrainer} can replay each committed MCL through its
 * hook under the correct context — routing to the same underlying database that produced it.
 *
 * <p>This is the hook-specific extension of the framework {@link OffloadContextResolver}. The
 * drain-side methods ({@link #groupKey}, {@link #resolveOpContext}) come from the framework
 * interface and are identical in shape across all offloads; the enqueue-side {@link #enrichKey}
 * stays here because its argument list is hook-specific ({@code opContext, hookId, mcl, sequence}).
 * The generic {@link com.linkedin.metadata.buffer.offload.OffloadDrainer} is parameterized by the
 * framework interface, so it drives the hook drainer without knowing {@link HookKey}.
 *
 * <p>Mirrors {@link com.linkedin.metadata.entity.retention.RetentionContextResolver}: the drainer
 * has no request context of its own (it runs on a scheduler thread). Without this seam, every
 * drained replay would run under a single system context and route to one fixed database, silently
 * missing entries that originated against a different route. The resolver captures enough routing
 * metadata at enqueue (when the request {@link OperationContext} is available) to reconstruct a
 * correct context at drain.
 *
 * <p>Default {@link SimpleHookContextResolver} is a no-op: keys carry no routing metadata ({@link
 * SimpleHookKey}) and {@link #resolveOpContext} returns the system context unchanged, matching the
 * single-database deployment. An extension may supply its own implementation (and a matching {@link
 * HookKey} subtype carrying route metadata), registered {@code @Primary} / {@code
 * @ConditionalOnMissingBean} to override the default.
 *
 * <p><b>Failure contract.</b> {@link #groupKey} and {@link #resolveOpContext} distinguish permanent
 * from transient failures: throw {@link UnresolvableHookKeyException} for a key that will
 * <em>never</em> resolve (e.g. a subtype the resolver does not produce — a wiring bug or a stale
 * rolling-deploy entry). The drainer drops such keys from the buffer so they don't re-throw every
 * tick. Any other {@link RuntimeException} is treated as transient: the drainer logs it and leaves
 * the key queued for retry on the next tick, so a transient blip cannot silently skip a hook
 * replay.
 */
public interface HookContextResolver extends OffloadContextResolver<HookKey> {

  /**
   * Build the buffer key for a committed MCL, attaching any routing metadata available on {@code
   * opContext}. Called at enqueue, where the request {@link OperationContext} is available.
   *
   * @param opContext the request context (may carry routing info for extensions)
   * @param hookId the plugin-config name of the hook that should replay this MCL
   * @param mcl the committed MCL (carries urn + aspectName + previous/current aspect)
   * @param sequence the globally-unique monotonic enqueue sequence (makes the key distinct)
   */
  @Nonnull
  HookKey enrichKey(
      @Nonnull OperationContext opContext,
      @Nonnull String hookId,
      @Nonnull MetadataChangeLog mcl,
      long sequence);

  /**
   * Stable grouping key for drained entries. The drainer groups drained entries by this so that
   * entries sharing a routing context are replayed together under one {@link #resolveOpContext}.
   * Implementations should return a value that is stable across calls for the same routing context
   * (e.g. a route id extracted from the key).
   */
  @Nonnull
  String groupKey(@Nonnull HookKey key);

  /**
   * Reconstruct the {@link OperationContext} to replay hooks for a drained group. Called once per
   * group (see {@link #groupKey}); the returned context is used for every entry in that group.
   *
   * @param key any key in the group (routing metadata is read from it)
   * @param systemOperationContext the bootstrap system context to base the result on
   */
  @Nonnull
  OperationContext resolveOpContext(
      @Nonnull HookKey key, @Nonnull OperationContext systemOperationContext);

  /**
   * No-op resolver used when no buffer is wired (and thus no drainer runs). {@link #enrichKey}
   * produces a plain {@link SimpleHookKey} with no routing metadata; {@link #resolveOpContext}
   * returns the passed system context unchanged. Only meaningful as a default field initializer —
   * when the buffer is {@link PostCommitHookBuffer#NO_OP}, {@link
   * PostCommitHookBuffer#defersApply()} is false so {@link #enrichKey} is never called.
   */
  HookContextResolver NO_OP =
      new HookContextResolver() {
        @Override
        public HookKey enrichKey(
            OperationContext opContext, String hookId, MetadataChangeLog mcl, long sequence) {
          String urn = mcl.getEntityUrn() == null ? "" : mcl.getEntityUrn().toString();
          String aspectName = mcl.getAspectName() == null ? "" : mcl.getAspectName();
          return new SimpleHookKey(hookId, urn, aspectName, sequence);
        }

        @Override
        public String groupKey(HookKey key) {
          return "default";
        }

        @Override
        public OperationContext resolveOpContext(
            HookKey key, OperationContext systemOperationContext) {
          return systemOperationContext;
        }
      };
}
