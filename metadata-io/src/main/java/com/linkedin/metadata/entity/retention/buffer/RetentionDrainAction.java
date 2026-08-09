package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.offload.DrainAction;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.entity.RetentionService;
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
 * a {@link com.linkedin.metadata.entity.retention.RetentionContextResolver#groupKey routing
 * context}) plus the per-group {@link OperationContext} reconstructed by the resolver, builds
 * {@link RetentionService.RetentionContext}s, and hands the whole group to {@link
 * RetentionService#applyRetentionBatchWithPolicyDefaults} in one call. Each (urn, aspect) pair runs
 * in its own transaction (where supported by the storage backend — see {@code
 * EbeanRetentionService}) so a poison pair fails and retries in isolation without blocking the
 * rest of the group.
 *
 * <p><b>Entry lifecycle is owned here.</b> The framework drainer only removes entries on a
 * <em>permanent</em> resolve failure; for everything else the action owns removal:
 *
 * <ul>
 *   <li>Malformed URN — {@link OffloadBuffer#removeIfSame} immediately so it doesn't wedge the
 *       drainer forever.
 *   <li>Committed (returned as a success by the service) — {@link OffloadBuffer#removeIfSame}.
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

    // Build retention contexts up front, keyed by the ORIGINAL drained key (kept for the
    // removeIfSame clear step). Malformed URNs are cleared immediately so they don't wedge the
    // drainer forever.
    List<RetentionService.RetentionContext> contexts = new ArrayList<>(entries.size());
    for (Map.Entry<RetentionKey, Long> entry : entries) {
      try {
        Urn urn = Urn.createFromString(entry.getKey().urn());
        contexts.add(
            RetentionService.RetentionContext.builder()
                .urn(urn)
                .aspectName(entry.getKey().aspectName())
                .maxVersion(Optional.of(entry.getValue()))
                .build());
      } catch (Exception e) {
        log.warn(
            "Skipping malformed retention key urn={} aspect={}; removing from buffer",
            entry.getKey().urn(),
            entry.getKey().aspectName(),
            e);
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
    if (contexts.isEmpty()) {
      return;
    }

    List<RetentionService.RetentionContext> successes;
    try {
      successes = retentionService.applyRetentionBatchWithPolicyDefaults(opContext, contexts);
    } catch (Exception e) {
      // Whole group failed (tx setup / commit). Nothing durable — throw so the framework leaves
      // the un-removed entries for retry (at-least-once). Malformed keys were already removed above.
      throw new RuntimeException("Retention group apply failed; leaving for retry", e);
    }

    if (metricUtils != null) {
      metricUtils.increment(RetentionDrainAction.class, "retention_drained", successes.size());
    }

    // Clear only the keys whose commits were durably committed. removeIfSame guards against a
    // higher version having re-merged into the buffer while we were draining. Match by RetentionKey
    // equals (the record's (urn, aspectName) equality).
    Set<RetentionKey> successKeys = new HashSet<>(successes.size());
    for (RetentionService.RetentionContext ctx : successes) {
      successKeys.add(new RetentionKey(ctx.getUrn().toString(), ctx.getAspectName()));
    }
    for (Map.Entry<RetentionKey, Long> entry : entries) {
      if (successKeys.contains(entry.getKey())) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
  }
}
