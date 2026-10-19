package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.CatchUpStatus;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.tasks.TaskInfo;

/**
 * Phase 2 non-blocking upgrade step that closes the write gap left by Phase 1 and by an entity
 * index fill. Queries aspects modified between the window start ({@code backfillStartTime} for a
 * filled index, else {@code reindexStartTime}) and {@code dualWriteStartTime}, falling back to now,
 * and emits MCLs via Kafka. The MAE consumer's update strategies (with rollback dual-write already
 * active) write those MCLs to the live index and any old backing index kept for rollback.
 *
 * <p>Only processes indices where {@code requiresDataBackfill=true} — settings-only changes don't
 * need this since the ES _reindex already copied all docs correctly.
 *
 * <p>Uses {@link ChangeType#RESTATE} so the consumer re-processes the full aspect state regardless
 * of diff mode optimizations. Every replayed change log is also marked as a catch-up in its headers
 * ({@link BackfillHeaders#MODE_HEADER_NAME}), which tells the V3 writer to rebuild the document
 * from primary storage as it stands when the event is processed, ignoring the row value carried by
 * the event, and to remove the entity from the index when it no longer exists there; see {@link
 * #catchUpChangeLog}.
 */
@Slf4j
public class IncrementalReindexCatchUpStep implements UpgradeStep {

  public static final String UPGRADE_ID_PREFIX = IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX;
  public static final String LAST_URN_KEY = IncrementalReindexState.CATCH_UP_LAST_URN;

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
            && !fillOutstanding(phase1State)
            && IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, catchUpState)) {
          log.info("Catch-up already completed for all indices, skipping");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        boolean anyIndexFailed = false;
        boolean fillLeftFailed = false;
        for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
          String indexName = entry.getKey();
          Map<String, String> indexState = entry.getValue();

          // A fill in flight, or one that failed, is caught up once it completes and the row is
          // COMPLETED. A verdict or resume cursor recorded while it was failed must not survive the
          // replan: both live on this upgrade result, which the replan does not clear, and a cursor
          // would skip the start of the rebuilt index's window. Leave the row without either until
          // then so that pass does not find it already terminal or partway through.
          IncrementalReindexState.Status phase1Status =
              IncrementalReindexState.getStatus(phase1State, indexName).orElse(null);
          if (phase1Status == IncrementalReindexState.Status.FAILED) {
            // Skipping the replay is not the same as the migration succeeding. Phase 1, which
            // replans a FAILED row, is not part of this non-blocking upgrade.
            fillLeftFailed = true;
          }
          if (fillNotReady(phase1Status)) {
            Map<String, String> cleared =
                IncrementalReindexState.clearCatchUpStatus(catchUpState, indexName);
            if (!cleared.equals(catchUpState)) {
              log.info(
                  "Index {} is not ready for catch-up ({}); dropping its catch-up progress",
                  indexName,
                  phase1Status);
              catchUpState = cleared;
              persistCatchUpCheckpoint(context, catchUpState, DataHubUpgradeState.IN_PROGRESS);
            } else {
              log.info(
                  "Index {} is not ready for catch-up ({}); its catch-up runs once the fill"
                      + " completes",
                  indexName,
                  phase1Status);
            }
            continue;
          }

          Optional<CatchUpStatus> existingCatchUpStatus =
              IncrementalReindexState.getCatchUpStatus(catchUpState, indexName);
          if (existingCatchUpStatus.isPresent() && existingCatchUpStatus.get().isTerminal()) {
            continue;
          }

          try {
            CatchUpStatus outcome = catchUpIndex(context, indexName, indexState);
            if (outcome.isTerminal() && isFilledFromPrimaryStorage(indexState)) {
              markMigrationComplete(indexName);
            }
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
        if (!buildIndicesConfig.isRollbackDualWriteEnabled()
            && !anyIndexFailed
            && !fillLeftFailed) {
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

        if (anyIndexFailed || fillLeftFailed) {
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
    Optional<Long> windowStart = IncrementalReindexState.catchUpWindowStart(indexState);
    if (windowStart.isEmpty()) {
      log.warn(
          "Index {} records neither backfillStartTime nor reindexStartTime, skipping catch-up",
          indexName);
      return CatchUpStatus.SKIPPED;
    }
    String dualWriteStartTimeStr = indexState.get(IncrementalReindexState.DUAL_WRITE_START_TIME);

    long fromEpochMs = windowStart.get();
    long toEpochMs =
        dualWriteStartTimeStr != null
            ? Long.parseLong(dualWriteStartTimeStr)
            : System.currentTimeMillis();

    if (fromEpochMs >= toEpochMs) {
      return CatchUpStatus.SKIPPED;
    }

    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String nextIndexName = indexState.get(IncrementalReindexState.NEXT_INDEX_NAME);
    Optional<String> entityNameOpt = indexConvention.getEntityName(opContext, indexName);

    if (entityNameOpt.isPresent()) {
      String entityName = entityNameOpt.get();
      log.info(
          "Catch-up for entity index {} (entity {}): window [{}, {}]",
          indexName,
          entityName,
          fromEpochMs,
          toEpochMs);
      emitMCLsForTimeRange(
          context,
          indexName,
          indexName + "." + LAST_URN_KEY,
          "urn:li:" + entityName + ":%",
          fromEpochMs,
          toEpochMs);
      return CatchUpStatus.COMPLETED;
    }

    if (indexConvention
        .entityIndexVersionOf(indexName)
        .filter(EntityIndexVersion.V3::equals)
        .isPresent()) {
      return catchUpV3Index(context, indexName, indexConvention, fromEpochMs, toEpochMs);
    }

    if (indexConvention.getEntityAndAspectName(opContext, indexName).isPresent()
        && nextIndexName != null) {
      String oldBackingIndexName = indexState.get(IncrementalReindexState.OLD_BACKING_INDEX_NAME);
      if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
        log.warn("Timeseries index {} has no oldBackingIndexName, skipping catch-up", indexName);
        return CatchUpStatus.SKIPPED;
      }
      log.info(
          "Catch-up for timeseries index {} (old: {}): _reindex window [{}, {}]",
          indexName,
          oldBackingIndexName,
          fromEpochMs,
          toEpochMs);
      return reindexTimeseriesGap(
          indexName, oldBackingIndexName, nextIndexName, fromEpochMs, toEpochMs);
    }

    if (isGlobalIndex(indexName)) {
      log.info("Catch-up for global index {}: window [{}, {}]", indexName, fromEpochMs, toEpochMs);
      emitMCLsForTimeRange(
          context, indexName, indexName + "." + LAST_URN_KEY, "%", fromEpochMs, toEpochMs);
      return CatchUpStatus.COMPLETED;
    }

    log.warn("Could not resolve index '{}' as entity, timeseries, or global, skipping", indexName);
    return CatchUpStatus.SKIPPED;
  }

  /**
   * A V3 index holds every entity type of its index key, so its window is replayed once per type in
   * name order, each type with its own resume cursor. Whether the window opened at a fill start or
   * a reindex start makes no difference here: the RESTATE reaches the alias either way.
   */
  private CatchUpStatus catchUpV3Index(
      UpgradeContext context,
      String indexName,
      IndexConvention indexConvention,
      long fromEpochMs,
      long toEpochMs) {
    Optional<String> indexKey = indexConvention.getEntityIndexKeyV3(opContext, indexName);
    if (indexKey.isEmpty()) {
      log.error(
          "Index {} is a V3 entity index whose key cannot be resolved; catch-up cannot scope it",
          indexName);
      return CatchUpStatus.FAILED;
    }
    List<String> entityNames =
        V3IndexKeys.entitySpecsForKey(opContext.getEntityRegistry(), indexKey.get()).stream()
            .map(EntitySpec::getName)
            .sorted()
            .collect(Collectors.toList());
    if (entityNames.isEmpty()) {
      log.error(
          "No entity types resolve to index key '{}' for {}; catch-up cannot scope it",
          indexKey.get(),
          indexName);
      return CatchUpStatus.FAILED;
    }
    log.info(
        "Catch-up for V3 index {} (key {}, {} entity type(s)): window [{}, {}]",
        indexName,
        indexKey.get(),
        entityNames.size(),
        fromEpochMs,
        toEpochMs);
    for (String entityName : entityNames) {
      emitMCLsForTimeRange(
          context,
          indexName,
          indexName + "." + entityName + "." + LAST_URN_KEY,
          "urn:li:" + entityName + ":%",
          fromEpochMs,
          toEpochMs);
    }
    return CatchUpStatus.COMPLETED;
  }

  private void persistCatchUpCheckpoint(
      UpgradeContext context, Map<String, String> catchUpState, DataHubUpgradeState state) {
    Map<String, String> merged =
        IncrementalReindexState.mergeCatchUpState(
            loadCurrentCheckpointState(context), catchUpState);
    context.upgrade().setUpgradeResult(opContext, upgradeIdUrn, entityService, state, merged);
  }

  /**
   * A fill that has not finished. Catch-up waits, and does not keep a verdict from this attempt.
   */
  private static boolean fillNotReady(@Nullable IncrementalReindexState.Status status) {
    return status == IncrementalReindexState.Status.FAILED
        || IncrementalReindexState.isBackfillStatus(status);
  }

  private static boolean fillOutstanding(@Nonnull Map<String, String> phase1State) {
    return IncrementalReindexState.getAllIndexStates(phase1State).keySet().stream()
        .anyMatch(
            indexName ->
                fillNotReady(
                    IncrementalReindexState.getStatus(phase1State, indexName).orElse(null)));
  }

  /**
   * The replayed change log for one stored aspect row, marked as a catch-up.
   *
   * <p>A replay is the only pass over its window: a writer that drops one of its events leaves a
   * hole nothing comes back to repair. The header tells the V3 writer so, and it lets a replayed
   * event's failure propagate instead of skipping the document the way it does for a live write
   * while V2 is dual-written. The V2 writer and the system-metadata and graph consumers do not read
   * the header and process the RESTATE as before.
   */
  private MetadataChangeLog catchUpChangeLog(@Nonnull final SystemAspect systemAspect) {
    final MetadataChangeLog changeLog =
        constructMCL(
            null,
            systemAspect.getUrn().getEntityType(),
            systemAspect.getUrn(),
            ChangeType.RESTATE,
            systemAspect.getAspectSpec().getName(),
            AuditStampUtils.createDefaultAuditStamp(),
            systemAspect.getRecordTemplate(),
            systemAspect
                .getSystemMetadata()
                .setRunId(id())
                .setLastObserved(System.currentTimeMillis()),
            null,
            null);
    changeLog.setHeaders(
        new StringMap(Map.of(BackfillHeaders.MODE_HEADER_NAME, BackfillHeaders.MODE_CATCH_UP)));
    return changeLog;
  }

  /**
   * Streams aspects (version 0) modified in the given time range and emits RESTATE MCLs for each.
   * Uses URN-based cursor pagination with per-index checkpointing for resumption.
   *
   * <p>MCLs are enqueued to Kafka without waiting per row. {@link Future#get()} is deferred until a
   * flush boundary (row count or byte threshold), then {@link EntityService#flushEventProducer()}
   * runs before checkpointing so resume state reflects fully sent batches.
   *
   * @param indexName the ES index name, for logging
   * @param lastUrnKey the checkpoint key this scan's resume URN is stored under; one per scan, so a
   *     V3 index replayed per entity type resumes each type independently
   * @param urnLikePattern the SQL LIKE pattern to scope the DB query (e.g. "urn:li:dataset:%" for
   *     entity-scoped, "%" for global indices like graph/system metadata)
   */
  private void emitMCLsForTimeRange(
      UpgradeContext context,
      String indexName,
      String lastUrnKey,
      String urnLikePattern,
      long fromEpochMs,
      long toEpochMs) {
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

    aspectDao.streamAspectBatches(
        opContext,
        args,
        stream -> {
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
                              systemAspect.getAspectSpec(),
                              catchUpChangeLog(systemAspect));
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
          return null;
        });

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
    String graphIndexName =
        indexConvention.getIndexName(
            opContext, SearchComponent.GRAPH, ElasticSearchGraphService.INDEX_NAME);
    String systemMetadataIndexName =
        indexConvention.getIndexName(
            opContext,
            SearchComponent.SYSTEM_METADATA,
            ElasticSearchSystemMetadataService.INDEX_NAME);
    return indexName.equals(graphIndexName) || indexName.equals(systemMetadataIndexName);
  }

  /** Whether the row's index was filled from primary storage, and so carries a migration mark. */
  private static boolean isFilledFromPrimaryStorage(Map<String, String> indexState) {
    return indexState.containsKey(IncrementalReindexState.BACKFILL_START_TIME);
  }

  /**
   * Marks the index as migrated: filled and caught up. The fill leaves the mark incomplete, because
   * this upgrade's rows belong to this version alone and a later deploy's Phase 1 reads the index,
   * not them: an index whose catch-up never finished is rebuilt there. A mark that cannot be
   * written fails the index, so a re-run replays what is left of the window and marks it again.
   */
  private void markMigrationComplete(String indexName) throws IOException {
    Pair<ESIndexBuilder, ReindexConfig> builderAndConfig = findIndexBuilderAndConfig(indexName);
    if (builderAndConfig == null) {
      throw new IllegalStateException(
          "No index builder found for " + indexName + "; its migration stays marked incomplete");
    }
    builderAndConfig.getFirst().markMigrationComplete(opContext, indexName);
  }

  @Nullable
  private Pair<ESIndexBuilder, ReindexConfig> findIndexBuilderAndConfig(String indexName) {
    for (ElasticSearchIndexed service : indexedServices) {
      try {
        for (ReindexConfig config : service.buildReindexConfigs(opContext, structuredProperties)) {
          if (config.name().equals(indexName)) {
            return Pair.of(service.getIndexBuilder(indexName), config);
          }
        }
      } catch (Exception e) {
        log.warn("Error checking service for index {}: {}", indexName, e.getMessage());
      }
    }
    return null;
  }
}
