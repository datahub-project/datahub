package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.script.Script;

/**
 * The write mirror of a family migration: every write the serving cluster receives is applied to
 * the migration target as well, under the target's index names.
 *
 * <p>This is a view over the serving cluster's processor (same shim, same bulk machinery), so the
 * serving write is byte-for-byte what it was without a migration. The mirrored write rides the
 * target cluster's own processor and therefore inherits its delivery semantics: ack-after-transfer,
 * retries and failure draining apply to it exactly as they apply to a primary write. A target
 * outage during a migration is a write outage for that family, by design; nothing here swallows it.
 * A by-query operation the serving cluster refused (the processor logs that and answers empty) is
 * not applied to the target either, and not logged: the serving cluster still holds what it
 * matched, so the target must too, and a replay must not take it away.
 *
 * <p>Family-scoped on purpose: one processor serves every family on a cluster, so wrapping the
 * cluster's processor would mirror families that are not migrating.
 */
@Slf4j
public class MirroredESBulkProcessor extends ESBulkProcessor {
  private static final String MIRRORED_WRITES_METRIC = "cluster_migration_mirrored_writes";

  private final ESBulkProcessor serving;
  private final ESBulkProcessor target;
  private final MigrationIndexNames names;
  @Nullable private final MirroredWriteLog writeLog;
  @Nullable private final MetricUtils metrics;

  /**
   * @param writeLog records deletes and by-query operations while the family's history is being
   *     copied; null for a mirror-only migration, which no copy follows
   */
  public MirroredESBulkProcessor(
      @Nonnull final ESBulkProcessor serving,
      @Nonnull final ESBulkProcessor target,
      @Nonnull final MigrationIndexNames names,
      @Nullable final MirroredWriteLog writeLog,
      @Nullable final MetricUtils metrics) {
    super(serving);
    if (serving == target) {
      throw new IllegalArgumentException("A family cannot be mirrored onto its serving processor");
    }
    this.serving = serving;
    this.target = target;
    this.names = names;
    this.writeLog = writeLog;
    this.metrics = metrics;
  }

  @Nonnull
  public ESBulkProcessor getServing() {
    return serving;
  }

  @Nonnull
  public ESBulkProcessor getTarget() {
    return target;
  }

  @Nonnull
  public MigrationIndexNames getNames() {
    return names;
  }

  @Override
  public ESBulkProcessor add(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull DocWriteRequest<?> request) {
    serving.add(opContext, urn, request);
    final String targetIndex = names.target(opContext, request.index());
    // Same routing key, so per-document ordering holds on the target as it does on the serving
    // side.
    target.add(opContext, urn, DocWriteRequests.withIndex(request, targetIndex));
    if (writeLog != null && request instanceof DeleteRequest) {
      writeLog.record(
          opContext, MirroredWriteLog.Kind.DELETE_BY_ID, targetIndex, request.id(), null);
    }
    mirrored();
    return this;
  }

  @Override
  public Optional<BulkByScrollResponse> deleteByQuery(
      @Nonnull OperationContext opContext,
      QueryBuilder queryBuilder,
      boolean refresh,
      int limit,
      TimeValue timeout,
      String... indices) {
    final Optional<BulkByScrollResponse> response =
        serving.deleteByQuery(opContext, queryBuilder, refresh, limit, timeout, indices);
    if (response.isEmpty()) {
      return response;
    }
    final String[] targets = targets(opContext, indices);
    target.deleteByQuery(opContext, queryBuilder, refresh, limit, timeout, targets);
    logByQuery(opContext, MirroredWriteLog.Kind.DELETE_BY_QUERY, targets, queryBuilder);
    mirrored();
    return response;
  }

  @Override
  public Optional<BulkByScrollResponse> updateByQuery(
      @Nonnull OperationContext opContext,
      Script script,
      QueryBuilder queryBuilder,
      String... indices) {
    final Optional<BulkByScrollResponse> response =
        serving.updateByQuery(opContext, script, queryBuilder, indices);
    if (response.isEmpty()) {
      return response;
    }
    final String[] targets = targets(opContext, indices);
    target.updateByQuery(opContext, script, queryBuilder, targets);
    logByQuery(opContext, MirroredWriteLog.Kind.UPDATE_BY_QUERY, targets, queryBuilder);
    mirrored();
    return response;
  }

  @Override
  public Optional<String> deleteByQueryAsync(
      @Nonnull OperationContext opContext,
      QueryBuilder queryBuilder,
      boolean refresh,
      int limit,
      @Nullable TimeValue timeout,
      String... indices) {
    final Optional<String> task =
        serving.deleteByQueryAsync(opContext, queryBuilder, refresh, limit, timeout, indices);
    if (task.isEmpty()) {
      return task;
    }
    final String[] targets = targets(opContext, indices);
    target.deleteByQueryAsync(opContext, queryBuilder, refresh, limit, timeout, targets);
    logByQuery(opContext, MirroredWriteLog.Kind.DELETE_BY_QUERY, targets, queryBuilder);
    mirrored();
    return task;
  }

  @Override
  public void flush() {
    serving.flush();
    target.flush();
  }

  @Override
  public void flushAndWait(Duration timeout) throws InterruptedException, TimeoutException {
    long failures = 0;
    BulkTransferException first = null;
    try {
      serving.flushAndWait(timeout);
    } catch (BulkTransferException e) {
      failures += e.getFailureCount();
      first = e;
    }
    try {
      target.flushAndWait(timeout);
    } catch (BulkTransferException e) {
      failures += e.getFailureCount();
      if (first == null) {
        first = e;
      } else {
        first.addSuppressed(e);
      }
    }
    if (first != null) {
      final BulkTransferException combined =
          new BulkTransferException(
              failures,
              "Bulk transfer completed with "
                  + failures
                  + " unrecovered item failure(s) across the serving and migration target clusters");
      combined.addSuppressed(first);
      throw combined;
    }
  }

  /** Flushes both sides; the registry owns and closes the underlying processors. */
  @Override
  public void close() throws IOException {
    flush();
  }

  @Override
  public boolean isAckAfterTransfer() {
    return serving.isAckAfterTransfer() || target.isAckAfterTransfer();
  }

  @Override
  public int getAckAfterTransferTimeoutSeconds() {
    return Math.max(
        serving.getAckAfterTransferTimeoutSeconds(), target.getAckAfterTransferTimeoutSeconds());
  }

  @Nonnull
  private String[] targets(@Nonnull final OperationContext opContext, final String... indices) {
    final String[] targets = new String[indices.length];
    for (int i = 0; i < indices.length; i++) {
      targets[i] = names.target(opContext, indices[i]);
    }
    return targets;
  }

  private void logByQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final MirroredWriteLog.Kind kind,
      @Nonnull final String[] targets,
      @Nullable final QueryBuilder query) {
    if (writeLog == null) {
      return;
    }
    for (String targetIndex : targets) {
      writeLog.record(opContext, kind, targetIndex, null, query);
    }
  }

  private void mirrored() {
    if (metrics != null) {
      metrics.increment(
          MirroredESBulkProcessor.class,
          MIRRORED_WRITES_METRIC + "_" + names.getFamily().getConfigKey(),
          1);
    }
  }
}
