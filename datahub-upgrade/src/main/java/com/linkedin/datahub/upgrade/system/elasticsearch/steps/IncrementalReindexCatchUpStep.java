package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.CatchUpStatus;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.tasks.TaskInfo;

/**
 * Phase 2 non-blocking upgrade step that closes the T0 gap created during Phase 1. For each
 * reindexed index it queries aspects modified in the window {@code [reindexStartTime,
 * catchUpEndTime)} and emits MCLs via Kafka, so both the current and next indices receive the
 * writes that landed on the old backing index during the reindex but were absent from the frozen
 * _reindex snapshot. Applies to every reindexed index (including settings-only reindexes): the T0
 * gap is a set of missing <em>documents</em> written during the reindex window, independent of
 * whether any field mapping changed.
 *
 * <p>The window's upper bound is resolved by {@link #resolveCatchUpEndTime} with a
 * most-precise-first fallback (aliasSwapTime → dualWriteStartTime → reindexCompleteTime + buffer →
 * now), always erring late so no T0-window write is missed.
 *
 * <p>Uses {@link ChangeType#RESTATE} so the consumer re-processes the full aspect state regardless
 * of diff mode optimizations.
 */
@Slf4j
public class IncrementalReindexCatchUpStep implements UpgradeStep {

  public static final String UPGRADE_ID_PREFIX = IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX;
  public static final String LAST_URN_KEY = "lastUrn";

  private static final String TIMESERIES_TIMESTAMP_FIELD = "@timestamp";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final String upgradeVersion;
  private final BuildIndicesConfiguration buildIndicesConfig;
  private final Urn upgradeIdUrn;
  private final Urn phase1UpgradeIdUrn;

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      String upgradeVersion,
      @Nullable BuildIndicesConfiguration buildIndicesConfig) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.upgradeVersion = upgradeVersion;
    this.buildIndicesConfig =
        buildIndicesConfig != null ? buildIndicesConfig : new BuildIndicesConfiguration();
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.phase1UpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion);
  }

  @Override
  public String id() {
    return UPGRADE_ID_PREFIX + "_" + upgradeVersion;
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        Optional<DataHubUpgradeResult> phase1Result =
            context.upgrade().getUpgradeResult(opContext, phase1UpgradeIdUrn, entityService);

        if (phase1Result.isEmpty() || phase1Result.get().getResult() == null) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        Map<String, String> phase1State = phase1Result.get().getResult();
        Map<String, Map<String, String>> allIndexStates =
            IncrementalReindexState.getAllIndexStates(phase1State);

        if (allIndexStates.isEmpty()) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        Optional<DataHubUpgradeResult> previousCatchUpResult =
            context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);
        Map<String, String> catchUpState =
            previousCatchUpResult
                .map(DataHubUpgradeResult::getResult)
                .map(HashMap::new)
                .orElseGet(HashMap::new);

        if (previousCatchUpResult.isPresent()
            && DataHubUpgradeState.SUCCEEDED.equals(previousCatchUpResult.get().getState())
            && IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, catchUpState)) {
          log.info("Catch-up already completed for all indices, skipping");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        boolean anyIndexFailed = false;
        for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
          String indexName = entry.getKey();
          Map<String, String> indexState = entry.getValue();

          Optional<CatchUpStatus> existingCatchUpStatus =
              IncrementalReindexState.getCatchUpStatus(catchUpState, indexName);
          if (existingCatchUpStatus.isPresent() && existingCatchUpStatus.get().isTerminal()) {
            continue;
          }

          try {
            CatchUpStatus outcome = catchUpIndex(context, indexName, indexState);
            catchUpState =
                IncrementalReindexState.setCatchUpStatus(catchUpState, indexName, outcome);
            persistCatchUpCheckpoint(context, catchUpState, DataHubUpgradeState.IN_PROGRESS);
            if (outcome == CatchUpStatus.FAILED) {
              anyIndexFailed = true;
            }
          } catch (Exception e) {
            log.error("Catch-up failed for index {}", indexName, e);
            catchUpState =
                IncrementalReindexState.setCatchUpStatus(
                    catchUpState, indexName, CatchUpStatus.FAILED);
            persistCatchUpCheckpoint(context, catchUpState, DataHubUpgradeState.IN_PROGRESS);
            anyIndexFailed = true;
          }
        }

        // If rollback dual-write is not enabled, mark all completed indices as
        // DUAL_WRITE_DISABLED to prevent a later enable of the flag from writing to stale or
        // deleted old indices.
        if (!buildIndicesConfig.isRollbackDualWriteEnabled() && !anyIndexFailed) {
          for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
            String indexName = entry.getKey();
            String status = entry.getValue().get(IncrementalReindexState.STATUS);
            Optional<CatchUpStatus> catchUpStatus =
                IncrementalReindexState.getCatchUpStatus(catchUpState, indexName);
            if (IncrementalReindexState.Status.COMPLETED.name().equals(status)
                && catchUpStatus.isPresent()
                && catchUpStatus.get() == CatchUpStatus.COMPLETED) {
              DataHubUpgradeState phaseState = phase1Result.get().getState();
              DataHubUpgradeResultConditionalPersist.mergeAndPersist(
                  opContext,
                  entityService,
                  phase1UpgradeIdUrn,
                  IncrementalReindexState.persistDualWriteDisabledMerge(indexName, phaseState));
              log.info(
                  "Marked index {} as DUAL_WRITE_DISABLED (rollbackDualWriteEnabled=false)",
                  indexName);
            }
          }
        }

        if (anyIndexFailed) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        persistCatchUpCheckpoint(context, catchUpState, DataHubUpgradeState.SUCCEEDED);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Throwable e) {
        log.error("IncrementalReindexCatchUpStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private CatchUpStatus catchUpIndex(
      UpgradeContext context, String indexName, Map<String, String> indexState) throws Throwable {
    String reindexStartTimeStr = indexState.get(IncrementalReindexState.REINDEX_START_TIME);
    String dualWriteStartTimeStr = indexState.get(IncrementalReindexState.DUAL_WRITE_START_TIME);

    if (reindexStartTimeStr == null) {
      log.warn("Index {} missing reindexStartTime, skipping catch-up", indexName);
      return CatchUpStatus.SKIPPED;
    }

    long reindexStartTime = Long.parseLong(reindexStartTimeStr);
    long catchUpEndTime = resolveCatchUpEndTime(indexName, indexState, dualWriteStartTimeStr);

    if (reindexStartTime >= catchUpEndTime) {
      return CatchUpStatus.SKIPPED;
    }

    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String nextIndexName = indexState.get(IncrementalReindexState.NEXT_INDEX_NAME);
    Optional<String> entityNameOpt = indexConvention.getEntityName(indexName);

    if (entityNameOpt.isPresent()) {
      String entityName = entityNameOpt.get();
      log.info(
          "Catch-up for entity index {} (entity {}): window [{}, {}]",
          indexName,
          entityName,
          reindexStartTime,
          catchUpEndTime);
      emitMCLsForTimeRange(
          context, indexName, "urn:li:" + entityName + ":%", reindexStartTime, catchUpEndTime);
      return CatchUpStatus.COMPLETED;
    }

    if (indexConvention.getEntityAndAspectName(indexName).isPresent() && nextIndexName != null) {
      String oldBackingIndexName = indexState.get(IncrementalReindexState.OLD_BACKING_INDEX_NAME);
      if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
        log.warn("Timeseries index {} has no oldBackingIndexName, skipping catch-up", indexName);
        return CatchUpStatus.SKIPPED;
      }
      log.info(
          "Catch-up for timeseries index {} (old: {}): _reindex window [{}, {}]",
          indexName,
          oldBackingIndexName,
          reindexStartTime,
          catchUpEndTime);
      return reindexTimeseriesGap(
          indexName, oldBackingIndexName, nextIndexName, reindexStartTime, catchUpEndTime);
    }

    if (isGlobalIndex(indexName)) {
      log.info(
          "Catch-up for global index {}: window [{}, {}]",
          indexName,
          reindexStartTime,
          catchUpEndTime);
      emitMCLsForTimeRange(context, indexName, "%", reindexStartTime, catchUpEndTime);
      return CatchUpStatus.COMPLETED;
    }

    log.warn("Could not resolve index '{}' as entity, timeseries, or global, skipping", indexName);
    return CatchUpStatus.SKIPPED;
  }

  /**
   * Resolves the exclusive upper bound of the T0 catch-up window, most-precise source first:
   *
   * <ol>
   *   <li>{@code aliasSwapTime} — the exact moment Phase 1 swapped the alias to the next index
   *       (only present on records written by the aliasSwapTime-aware Phase 1).
   *   <li>{@code dualWriteStartTime} — when the MAE consumer first dual-wrote; recorded at/after
   *       the swap, so a safe (never-early) over-estimate.
   *   <li>{@code reindexCompleteTime + buffer} — for legacy records lacking both timestamps above.
   *       The swap happens shortly after reindex completion, gated by up to two blocking {@code
   *       getCount} refreshes each bounded by {@code slowOperationTimeoutSeconds}; a buffer of
   *       {@code 2 × slowOperationTimeoutSeconds} guarantees the bound is at/after the real swap.
   *   <li>{@code now} — last resort if reindex completion was never recorded.
   * </ol>
   *
   * <p>Every fallback deliberately errs late: over-estimating only replays already-present writes
   * (RESTATE is idempotent), whereas under-estimating would leave T0-window writes missing from the
   * next index.
   */
  private long resolveCatchUpEndTime(
      String indexName, Map<String, String> indexState, @Nullable String dualWriteStartTimeStr) {
    String aliasSwapTimeStr = indexState.get(IncrementalReindexState.ALIAS_SWAP_TIME);
    if (aliasSwapTimeStr != null) {
      return Long.parseLong(aliasSwapTimeStr);
    }
    if (dualWriteStartTimeStr != null) {
      return Long.parseLong(dualWriteStartTimeStr);
    }
    String reindexCompleteTimeStr = indexState.get(IncrementalReindexState.REINDEX_COMPLETE_TIME);
    if (reindexCompleteTimeStr != null) {
      long bufferMs = 2L * buildIndicesConfig.getSlowOperationTimeoutSeconds() * 1000L;
      long end = Long.parseLong(reindexCompleteTimeStr) + bufferMs;
      log.info(
          "Index {} has no aliasSwapTime/dualWriteStartTime; using reindexCompleteTime + {}ms buffer as catch-up end ({})",
          indexName,
          bufferMs,
          end);
      return end;
    }
    log.warn(
        "Index {} has no aliasSwapTime/dualWriteStartTime/reindexCompleteTime; falling back to now for catch-up end",
        indexName);
    return System.currentTimeMillis();
  }

  private void persistCatchUpCheckpoint(
      UpgradeContext context, Map<String, String> catchUpState, DataHubUpgradeState state) {
    Map<String, String> merged = loadCurrentCheckpointState(context);
    merged.putAll(catchUpState);
    context.upgrade().setUpgradeResult(opContext, upgradeIdUrn, entityService, state, merged);
  }

  /**
   * Streams aspects (version 0) modified in the given time range and emits RESTATE MCLs for each.
   * Uses URN-based cursor pagination with per-index checkpointing for resumption.
   *
   * <p>MCLs are enqueued to Kafka without waiting per row. {@link Future#get()} is deferred until a
   * flush boundary (row count or byte threshold), then {@link EntityService#flushEventProducer()}
   * runs before checkpointing so resume state reflects fully sent batches.
   *
   * @param indexName the ES index name, used as a prefix for per-index resume state
   * @param urnLikePattern the SQL LIKE pattern to scope the DB query (e.g. "urn:li:dataset:%" for
   *     entity-scoped, "%" for global indices like graph/system metadata)
   */
  private void emitMCLsForTimeRange(
      UpgradeContext context,
      String indexName,
      String urnLikePattern,
      long fromEpochMs,
      long toEpochMs) {
    String lastUrnKey = indexName + "." + LAST_URN_KEY;

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);
    String resumeUrn =
        prevResult
            .filter(
                result ->
                    DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                        && result.getResult() != null
                        && result.getResult().containsKey(lastUrnKey))
            .map(result -> result.getResult().get(lastUrnKey))
            .orElse(null);

    if (resumeUrn != null) {
      log.info("Resuming catch-up for index {} from URN: {}", indexName, resumeUrn);
    }

    int sqlPageSize = buildIndicesConfig.getCatchUpSqlPageSize();
    int flushInterval = buildIndicesConfig.getCatchUpFlushInterval();
    long flushBytesThreshold = buildIndicesConfig.getCatchUpFlushBytesThreshold();

    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .batchSize(sqlPageSize)
            .gePitEpochMs(fromEpochMs)
            .lePitEpochMs(toEpochMs)
            .urnLike(urnLikePattern)
            .lastUrn(resumeUrn)
            .urnBasedPagination(true);

    FlushTracker tracker = new FlushTracker();

    try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(opContext, args)) {
      stream
          .partition(sqlPageSize)
          .forEach(
              page -> {
                List<EbeanAspectV2> pageAspects = page.collect(Collectors.toList());

                List<SystemAspect> systemAspects =
                    EntityUtils.toSystemAspectFromEbeanAspects(
                        opContext, opContext.getRetrieverContext(), pageAspects);

                for (int i = 0; i < systemAspects.size(); i++) {
                  SystemAspect systemAspect = systemAspects.get(i);
                  if (flushBytesThreshold > 0) {
                    tracker.bytesSinceLastFlush +=
                        metadataColumnCharLength(pageAspects.get(i).getMetadata());
                  }

                  Pair<Future<?>, Boolean> future =
                      entityService.alwaysProduceMCLAsync(
                          opContext,
                          systemAspect.getUrn(),
                          systemAspect.getUrn().getEntityType(),
                          systemAspect.getAspectSpec().getName(),
                          systemAspect.getAspectSpec(),
                          null,
                          systemAspect.getRecordTemplate(),
                          null,
                          systemAspect
                              .getSystemMetadata()
                              .setRunId(id())
                              .setLastObserved(System.currentTimeMillis()),
                          AuditStampUtils.createDefaultAuditStamp(),
                          ChangeType.RESTATE);
                  tracker.pendingFutures.add(future.getFirst());
                  tracker.lastProcessedAspect = systemAspect;
                  tracker.rowsSinceLastFlush++;

                  if (shouldFlush(
                      tracker.rowsSinceLastFlush,
                      tracker.bytesSinceLastFlush,
                      flushInterval,
                      flushBytesThreshold)) {
                    awaitPendingAndFlush(context, indexName, lastUrnKey, tracker);
                  }
                }
              });
    }

    if (tracker.rowsSinceLastFlush > 0 || !tracker.pendingFutures.isEmpty()) {
      awaitPendingAndFlush(context, indexName, lastUrnKey, tracker);
    }
  }

  static boolean shouldFlush(
      int rowsSinceLastFlush,
      long bytesSinceLastFlush,
      int flushInterval,
      long flushBytesThreshold) {
    if (rowsSinceLastFlush >= flushInterval) {
      return true;
    }
    return flushBytesThreshold > 0 && bytesSinceLastFlush >= flushBytesThreshold;
  }

  /**
   * Raw UTF-16 code-unit count of the SQL {@code metadata} column already loaded on the row. Used
   * as a cheap proxy for serialized MCL payload size (no parsing or re-serialization).
   */
  static int metadataColumnCharLength(@Nullable String metadata) {
    return metadata == null ? 0 : metadata.length();
  }

  private void awaitPendingAndFlush(
      UpgradeContext context, String indexName, String lastUrnKey, FlushTracker tracker) {
    awaitPendingFutures(tracker.pendingFutures);
    flushAndCheckpoint(context, indexName, lastUrnKey, tracker);
  }

  private static void awaitPendingFutures(List<Future<?>> pendingFutures) {
    for (Future<?> future : pendingFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    pendingFutures.clear();
  }

  private void flushAndCheckpoint(
      UpgradeContext context, String indexName, String lastUrnKey, FlushTracker tracker) {
    if (tracker.lastProcessedAspect == null) {
      return;
    }
    entityService.flushEventProducer();
    Map<String, String> checkpoint = loadCurrentCheckpointState(context);
    checkpoint.put(lastUrnKey, tracker.lastProcessedAspect.getUrn().toString());
    persistCatchUpCheckpoint(context, checkpoint, DataHubUpgradeState.IN_PROGRESS);
    tracker.rowsSinceLastFlush = 0;
    tracker.bytesSinceLastFlush = 0;
  }

  private static final class FlushTracker {
    int rowsSinceLastFlush = 0;
    long bytesSinceLastFlush = 0;
    SystemAspect lastProcessedAspect = null;
    final List<Future<?>> pendingFutures = new ArrayList<>();
  }

  private Map<String, String> loadCurrentCheckpointState(UpgradeContext context) {
    Optional<DataHubUpgradeResult> current =
        context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);
    if (current.isPresent() && current.get().getResult() != null) {
      return new HashMap<>(current.get().getResult());
    }
    return new HashMap<>();
  }

  /**
   * Copies timeseries documents from the old backing index to the next index for the T0 gap window
   * using a filtered ES _reindex. Timeseries data is not in SQL so MCL-based catch-up doesn't work.
   * Polls until the reindex completes and throws on failure.
   *
   * @param aliasName the index alias name (for looking up the index builder and config)
   * @param oldBackingIndex the physical old backing index that received writes during Phase 1
   * @param nextIndex the physical next index created by Phase 1
   */
  private CatchUpStatus reindexTimeseriesGap(
      String aliasName, String oldBackingIndex, String nextIndex, long fromEpochMs, long toEpochMs)
      throws Throwable {
    Pair<ESIndexBuilder, ReindexConfig> builderAndConfig = findIndexBuilderAndConfig(aliasName);
    if (builderAndConfig == null) {
      log.warn("No index builder found for timeseries index {}, skipping catch-up", aliasName);
      return CatchUpStatus.SKIPPED;
    }

    ESIndexBuilder indexBuilder = builderAndConfig.getFirst();
    ReindexConfig config = builderAndConfig.getSecond();
    int targetShards = ESIndexBuilder.extractTargetShards(config);

    RangeQueryBuilder timeRangeFilter =
        QueryBuilders.rangeQuery(TIMESERIES_TIMESTAMP_FIELD).gte(fromEpochMs).lt(toEpochMs);

    try {
      String taskId =
          indexBuilder.submitFilteredReindex(
              opContext, oldBackingIndex, nextIndex, timeRangeFilter, targetShards);
      log.info(
          "Submitted timeseries catch-up _reindex for {} -> {} (task {}, shards {})",
          oldBackingIndex,
          nextIndex,
          taskId,
          targetShards);
    } catch (Exception e) {
      if (isIndexNotFound(e)) {
        log.warn(
            "Old backing index {} missing for timeseries catch-up on {}, skipping",
            oldBackingIndex,
            aliasName);
        return CatchUpStatus.SKIPPED;
      }
      throw e;
    }

    long timeoutAt = indexBuilder.computeTimeoutAt();
    long pollIntervalMs = buildIndicesConfig.getTaskPollIntervalSeconds() * 1000;

    while (System.currentTimeMillis() < timeoutAt) {
      Optional<TaskInfo> runningTask = indexBuilder.getTaskInfoByHeader(opContext, oldBackingIndex);
      if (runningTask.isEmpty()) {
        log.info("Timeseries catch-up _reindex completed for {} -> {}", oldBackingIndex, nextIndex);
        return CatchUpStatus.COMPLETED;
      }
      log.info(
          "Timeseries catch-up _reindex still running for {} -> {}", oldBackingIndex, nextIndex);
      Thread.sleep(pollIntervalMs);
    }

    throw new RuntimeException(
        "Timeseries catch-up _reindex timed out for " + oldBackingIndex + " -> " + nextIndex);
  }

  private static boolean isIndexNotFound(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains("index_not_found_exception")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Returns true for indices that are not entity-scoped and require a global (all-entity) catch-up.
   * These are the graph and system metadata indices.
   */
  private boolean isGlobalIndex(String indexName) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String graphIndexName = indexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME);
    String systemMetadataIndexName =
        indexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME);
    return indexName.equals(graphIndexName) || indexName.equals(systemMetadataIndexName);
  }

  @Nullable
  private Pair<ESIndexBuilder, ReindexConfig> findIndexBuilderAndConfig(String indexName) {
    for (ElasticSearchIndexed service : indexedServices) {
      try {
        for (ReindexConfig config : service.buildReindexConfigs(opContext, structuredProperties)) {
          if (config.name().equals(indexName)) {
            return Pair.of(service.getIndexBuilder(), config);
          }
        }
      } catch (Exception e) {
        log.warn("Error checking service for index {}: {}", indexName, e.getMessage());
      }
    }
    return null;
  }
}
