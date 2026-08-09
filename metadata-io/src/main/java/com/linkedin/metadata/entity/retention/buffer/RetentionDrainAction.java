package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.offload.DrainAction;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Retention-specific replay for the framework {@link
 * com.linkedin.metadata.buffer.offload.OffloadDrainer}. Receives one drained group (entries sharing
 * a routing context) plus the per-group {@link OperationContext} reconstructed by the resolver,
 * builds {@link RetentionBatchEntry}s bundling each original {@link RetentionKey} with its {@link
 * RetentionService.RetentionContext}, and hands the whole group to {@link
 * RetentionService#applyRetentionBatchWithPolicyDefaults} in one call. Each (urn, aspect) pair runs
 * in its own transaction (where supported by the storage backend — see {@code
 * EbeanRetentionService}) so a poison pair fails and retries in isolation without blocking the rest
 * of the group.
 *
 * <p><b>Original-key preservation.</b> The service echoes back the committed {@link RetentionKey}s
 * (original instances, not reconstructed ones), so the cross-off clear is by {@link RetentionKey}
 * equals (explicit per subtype). A key subtype that carries routing metadata is removed with that
 * metadata intact, and two requests for the same URN routed to different underlying databases do
 * not cross-clear.
 *
 * <p><b>Entry lifecycle is owned here.</b> The framework drainer only removes entries on a
 * <em>permanent</em> resolve failure; for everything else the action owns removal:
 *
 * <ul>
 *   <li>Malformed URN — {@link OffloadBuffer#removeIfSame} immediately so it doesn't wedge the
 *       drainer forever.
 *   <li>Committed (returned by the service) — {@link OffloadBuffer#removeIfSame}.
 *   <li>Whole-group apply throws — the action throws without removing anything; the framework
 *       leaves the un-removed entries for the next tick (at-least-once). Any entries the action
 *       finished before throwing must have been removed already.
 * </ul>
 */
@Slf4j
public class RetentionDrainAction implements DrainAction<RetentionKey, Long> {

  private final RetentionService<?> retentionService;
  @Nullable private final MetricUtils metricUtils;

  public RetentionDrainAction(
      @Nonnull RetentionService<?> retentionService, @Nullable MetricUtils metricUtils) {
    this.retentionService = retentionService;
    this.metricUtils = metricUtils;
  }

  @Override
  public void apply(
      @Nonnull List<Map.Entry<RetentionKey, Long>> entries,
      @Nonnull OperationContext opContext,
      @Nonnull OffloadBuffer<RetentionKey, Long> buffer) {
    if (entries.isEmpty()) {
      return;
    }

    List<RetentionBatchEntry> batchEntries = new ArrayList<>(entries.size());
    for (Map.Entry<RetentionKey, Long> entry : entries) {
      try {
        Urn urn = Urn.createFromString(entry.getKey().urn());
        RetentionService.RetentionContext context =
            RetentionService.RetentionContext.builder()
                .urn(urn)
                .aspectName(entry.getKey().aspectName())
                .maxVersion(Optional.of(entry.getValue()))
                .build();
        batchEntries.add(new RetentionBatchEntry(entry.getKey(), context));
      } catch (Exception e) {
        log.warn(
            "Skipping malformed retention key urn={} aspect={}; removing from buffer",
            entry.getKey().urn(),
            entry.getKey().aspectName(),
            e);
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
    if (batchEntries.isEmpty()) {
      return;
    }

    List<RetentionKey> successes;
    try {
      successes = retentionService.applyRetentionBatchWithPolicyDefaults(opContext, batchEntries);
    } catch (Exception e) {
      throw new RuntimeException("Retention group apply failed; leaving for retry", e);
    }

    if (metricUtils != null) {
      metricUtils.increment(RetentionDrainAction.class, "retention_drained", successes.size());
    }

    Set<RetentionKey> successKeys = new HashSet<>(successes);
    for (Map.Entry<RetentionKey, Long> entry : entries) {
      if (successKeys.contains(entry.getKey())) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
  }
}
