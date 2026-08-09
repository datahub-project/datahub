package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.buffer.offload.DrainAction;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * The hook-specific {@link DrainAction}: replays a drained group of committed MCLs through their
 * one hook and feeds the generated MCPs to the {@link PostCommitHookSink} for re-ingest. This is
 * the ONLY hook-specific piece of the async replay path — all drain infra (lock, paging, CAS,
 * scheduling) comes from the framework {@link com.linkedin.metadata.buffer.offload.OffloadDrainer}.
 *
 * <p><b>Retry / DLQ.</b> Generation (hook execution) and emit (re-ingest) are separated:
 *
 * <ul>
 *   <li>A group whose generation throws is retried per-MCL ({@link #replaySingle}) so one poison
 *       MCL cannot wedge the rest of the group.
 *   <li>A group whose generation succeeds is emitted; on emit failure the entries stay in the
 *       buffer for the next tick (at-least-once → possible duplicate re-emit next tick, accepted
 *       because downstream ingest is idempotent for these hooks). The drainer does NOT re-run
 *       generation on emit failure, so there is no immediate double-emit.
 *   <li>A per-MCL generation failure increments the entry's retry count; once {@link
 *       HookPayload#isPoison()} the key is dropped + metric'd (v1 has no DLQ table, matching the
 *       retention drainer — pending work is recoverable via a documented re-sync since hooks
 *       re-derive from current state).
 * </ul>
 *
 * <p><b>No coalescing:</b> every drained entry is a distinct committed MCL (unique sequence in its
 * {@link HookKey}); nothing is collapsed, so no intermediate transition is ever dropped.
 */
@Slf4j
public class HookDrainAction implements DrainAction<HookKey, HookPayload> {

  private final PostCommitHookSink sink;
  @Nullable private final MetricUtils metricUtils;

  public HookDrainAction(@Nonnull PostCommitHookSink sink, @Nullable MetricUtils metricUtils) {
    this.sink = sink;
    this.metricUtils = metricUtils;
  }

  @Override
  public void apply(
      @Nonnull List<Map.Entry<HookKey, HookPayload>> entries,
      @Nonnull OperationContext opContext,
      @Nonnull OffloadBuffer<HookKey, HookPayload> buffer) {
    HookKey firstKey = entries.get(0).getKey();
    RetrieverContext retrieverContext = opContext.getRetrieverContext();
    EntityRegistry entityRegistry = retrieverContext.getAspectRetriever().getEntityRegistry();

    MCPSideEffect hook = findHook(entityRegistry, firstKey.getHookId());
    if (hook == null) {
      log.warn(
          "Post-commit hook '{}' not found in registry; dropping {} pending replays",
          firstKey.getHookId(),
          entries.size());
      buffer.removeAll(entries);
      increment("post_commit_hook_missing", entries.size());
      return;
    }

    List<MCPItem> generated;
    try {
      List<MCLItem> mclItems = buildMclItems(retrieverContext, entries);
      generated =
          hook.postApply(opContext, mclItems, retrieverContext).collect(Collectors.toList());
    } catch (Throwable t) {
      log.warn(
          "Post-commit hook '{}' group generation failed ({} MCLs); isolating per-MCL",
          hook.getConfig().getClassName(),
          entries.size(),
          t);
      for (Map.Entry<HookKey, HookPayload> e : entries) {
        replaySingle(opContext, retrieverContext, hook, e, buffer);
      }
      return;
    }

    try {
      if (!generated.isEmpty()) {
        sink.emit(opContext, generated);
      }
    } catch (Throwable t) {
      log.warn(
          "Post-commit hook '{}' emit failed for {} entries; leaving for retry (at-least-once)",
          hook.getConfig().getClassName(),
          entries.size(),
          t);
      increment("post_commit_hook_emit_failed", entries.size());
      return;
    }

    // Success: no requeue happened in this branch, so a non-CAS batch remove is safe (one
    // IMap.removeAll round-trip instead of N per-entry CAS removes). The retry path above
    // (replaySingle) keeps per-entry removeIfSame because it remove-then-requeues the same key.
    buffer.removeAll(entries);
    increment("post_commit_hook_replayed", entries.size());
  }

  private void replaySingle(
      @Nonnull OperationContext opContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull MCPSideEffect hook,
      @Nonnull Map.Entry<HookKey, HookPayload> entry,
      @Nonnull OffloadBuffer<HookKey, HookPayload> buffer) {
    HookPayload payload = entry.getValue();
    try {
      MetadataChangeLog mcl = payload.toMcl();
      MCLItem mclItem = MCLItemImpl.builder().build(mcl, retrieverContext.getAspectRetriever());
      List<MCPItem> generated =
          hook.postApply(opContext, List.of(mclItem), retrieverContext)
              .collect(Collectors.toList());
      if (!generated.isEmpty()) {
        sink.emit(opContext, generated);
      }
      buffer.removeIfSame(entry.getKey(), payload);
      increment("post_commit_hook_replayed", 1);
    } catch (Throwable t) {
      HookPayload next = payload.incrementRetry();
      if (next.isPoison()) {
        log.warn(
            "Post-commit hook '{}' poison MCL after {} attempts; dropping urn={} aspect={}",
            hook.getConfig().getClassName(),
            next.getRetryCount(),
            entry.getKey().getUrn(),
            entry.getKey().getAspectName(),
            t);
        buffer.removeIfSame(entry.getKey(), payload);
        increment("post_commit_hook_poison_dropped", 1);
      } else {
        log.warn(
            "Post-commit hook '{}' replay failed (attempt {}); leaving for retry urn={} aspect={}",
            hook.getConfig().getClassName(),
            next.getRetryCount(),
            entry.getKey().getUrn(),
            entry.getKey().getAspectName(),
            t);
        buffer.removeIfSame(entry.getKey(), payload);
        buffer.requeue(entry.getKey(), next);
        increment("post_commit_hook_replay_failed", 1);
      }
    }
  }

  @Nullable
  private static MCPSideEffect findHook(
      @Nonnull EntityRegistry entityRegistry, @Nonnull String hookId) {
    return entityRegistry.getAllMCPSideEffects().stream()
        .filter(h -> hookId.equals(h.getConfig().getClassName()))
        .findFirst()
        .orElse(null);
  }

  private static List<MCLItem> buildMclItems(
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull List<Map.Entry<HookKey, HookPayload>> entries) {
    List<MCLItem> items = new ArrayList<>(entries.size());
    for (Map.Entry<HookKey, HookPayload> e : entries) {
      MetadataChangeLog mcl = e.getValue().toMcl();
      items.add(MCLItemImpl.builder().build(mcl, retrieverContext.getAspectRetriever()));
    }
    return items;
  }

  private void increment(@Nonnull String metric, long n) {
    if (metricUtils != null) {
      metricUtils.increment(HookDrainAction.class, metric, n);
    }
  }
}
