package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

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
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.CatchUpStatus;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.UnknownDataGuard;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
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
 * the event, and to remove the entity when it no longer exists there; see {@link
 * #catchUpChangeLog}. Once a V3 index's window is replayed, the tombstones its fill left are
 * removed.
 */
@Slf4j
public class IncrementalReindexCatchUpStep implements UpgradeStep {
  private static final UnknownDataGuard UNKNOWN_DATA =
      UnknownDataGuard.forSite(IncrementalReindexCatchUpStep.class, "row (left unindexed)");

  public static final String UPGRADE_ID_PREFIX = IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX;
  public static final String LAST_URN_KEY = IncrementalReindexState.CATCH_UP_LAST_URN;

  private static final String TIMESERIES_TIMESTAMP_FIELD = "@timestamp";

  // Consecutive incomplete _tasks responses tolerated before the catch-up poll fails closed. The
  // loop cannot rely on the reindex timeout to bound this: it is Long.MAX_VALUE under the default
  // maxReindexHours=0. At the default 5s poll interval this is ~5 minutes of a cluster unable to
  // answer _tasks; a single trustworthy read resets it.
  static final int MAX_CONSECUTIVE_INCOMPLETE_TASK_LISTS = 60;

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Supplier<Set<Pair<Urn, StructuredPropertyDefinition>>> structuredProperties;
  private final String upgradeVersion;
  private final BuildIndicesConfiguration buildIndicesConfig;
  @Nullable private final UpdateIndicesService updateIndicesService;
  private final Urn upgradeIdUrn;
  private final Urn phase1UpgradeIdUrn;

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      List<ElasticSearchIndexed> indexedServices,
      Supplier<Set<Pair<Urn, StructuredPropertyDefinition>>> structuredProperties,
      String upgradeVersion,
      @Nullable BuildIndicesConfiguration buildIndicesConfig,
      @Nullable UpdateIndicesService updateIndicesService) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.upgradeVersion = upgradeVersion;
    this.buildIndicesConfig =
        buildIndicesConfig != null ? buildIndicesConfig : new BuildIndicesConfiguration();
    this.updateIndicesService = updateIndicesService;
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

        Map<String, Pair<ESIndexBuilder, ReindexConfig>> indexBuildersAndConfigs =
            buildIndexBuildersAndConfigs();
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
            CatchUpStatus outcome =
                catchUpIndex(context, indexName, indexState, indexBuildersAndConfigs);
            if (outcome.isTerminal() && isFilledFromPrimaryStorage(indexState)) {
              markMigrationComplete(indexName, indexBuildersAndConfigs);
            }
            catchUpState = loadCurrentCheckpointState(context);
            catchUpState =
                IncrementalReindexState.setCatchUpStatus(catchUpState, indexName, outcome);
            persistCatchUpCheckpoint(context, catchUpState, DataHubUpgradeState.IN_PROGRESS);
            if (outcome == CatchUpStatus.FAILED) {
              anyIndexFailed = true;
            }
          } catch (Exception e) {
            log.error("Catch-up failed for index {}", indexName, e);
            catchUpState = loadCurrentCheckpointState(context);
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

        Map<String, String> finalCheckpoint =
            IncrementalReindexState.mergeCatchUpState(
                loadCurrentCheckpointState(context), catchUpState);
        persistCatchUpCheckpoint(context, finalCheckpoint, DataHubUpgradeState.SUCCEEDED);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Throwable e) {
        log.error("IncrementalReindexCatchUpStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private CatchUpStatus catchUpIndex(
      UpgradeContext context,
      String indexName,
      Map<String, String> indexState,
      Map<String, Pair<ESIndexBuilder, ReindexConfig>> indexBuildersAndConfigs)
      throws Throwable {
    Optional<Long> windowStart = IncrementalReindexState.catchUpWindowStart(indexState);
    if (windowStart.isEmpty()) {
      log.warn(
          "Index {} records neither backfillStartTime nor reindexStartTime, skipping catch-up",
          indexName);
      return skipIndex(indexName);
    }
    String dualWriteStartTimeStr = indexState.get(IncrementalReindexState.DUAL_WRITE_START_TIME);

    long fromEpochMs = windowStart.get();
    long toEpochMs =
        dualWriteStartTimeStr != null
            ? Long.parseLong(dualWriteStartTimeStr)
            : System.currentTimeMillis();

    if (fromEpochMs >= toEpochMs) {
      return skipIndex(indexName);
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
          indexName,
          oldBackingIndexName,
          nextIndexName,
          fromEpochMs,
          toEpochMs,
          indexBuildersAndConfigs);
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
   * a reindex start makes no difference here: the RESTATE reaches the alias at a live generation
   * either way.
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
    return removeV3Tombstones(indexName) ? CatchUpStatus.COMPLETED : CatchUpStatus.FAILED;
  }

  /**
   * A V3 index whose replay is skipped still drops the tombstones its fill left. There is no window
   * to replay, and the fill is already done, so nothing can bring those entities back. Any other
   * index is skipped as before.
   */
  private CatchUpStatus skipIndex(String indexName) {
    if (isV3EntityIndex(indexName) && !removeV3Tombstones(indexName)) {
      return CatchUpStatus.FAILED;
    }
    return CatchUpStatus.SKIPPED;
  }

  private boolean isV3EntityIndex(String indexName) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .entityIndexVersionOf(indexName)
        .filter(EntityIndexVersion.V3::equals)
        .isPresent();
  }

  /**
   * Removes the tombstones a fill left in the index. A hard delete during the fill leaves one in
   * the index the fill writes, so the create-only fill cannot bring the entity back; with the fill
   * done nothing can, and catch-up replays delete rather than tombstone. This runs only once the
   * fill has completed: an index whose fill is paused, in progress or failed is not caught up at
   * all. A cleanup that does not finish fails catch-up, so a re-run removes what is left.
   */
  private boolean removeV3Tombstones(String indexName) {
    if (updateIndicesService != null) {
      try {
        updateIndicesService.deleteV3Tombstones(opContext, indexName);
        return true;
      } catch (RuntimeException e) {
        log.error("Removing V3 tombstones from {} failed", indexName, e);
        return false;
      }
    }
    Optional<ElasticSearchService> searchService =
        indexedServices.stream()
            .filter(ElasticSearchService.class::isInstance)
            .map(ElasticSearchService.class::cast)
            .findFirst();
    if (searchService.isEmpty()) {
      log.error("No entity search service to remove V3 tombstones from {}", indexName);
      return false;
    }
    try {
      ESWriteDAO.DeleteByQueryResult result =
          searchService.get().deleteV3Tombstones(opContext, indexName);
      if (!result.isSuccess()) {
        log.error(
            "Removing V3 tombstones from {} did not finish ({} remaining): {}",
            indexName,
            result.getRemainingDocuments(),
            result.getFailureReason());
        return false;
      }
      return true;
    } catch (RuntimeException e) {
      log.error("Removing V3 tombstones from {} failed", indexName, e);
      return false;
    }
  }

  private void persistCatchUpCheckpoint(
      UpgradeContext context, Map<String, String> catchUpState, DataHubUpgradeState state) {
    context.upgrade().setUpgradeResult(opContext, upgradeIdUrn, entityService, state, catchUpState);
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
   * <p>The row was read from a snapshot of primary storage, and between that read and the MAE
   * processing the event the entity can be hard-deleted and recreated, which restarts its aspect
   * versions at 1. The V3 writer hydrates the document from primary storage and overlays a plain
   * event unless the event is verifiably older than what it hydrated; a pre-delete row outranks the
   * recreated aspect by version, and because {@code lastObserved} is stamped at produce time the
   * writer's timestamp guard cannot tell either, so the pre-delete value would be written as a
   * full-document replace at a new live generation and stick. Marked as a catch-up, the event makes
   * the writer rebuild from primary storage as it stands at processing time and skip the overlay
   * altogether, or remove the entity when it is gone, which is what a replay is for. The V2 writer
   * and the system-metadata and graph consumers do not read the header and process the RESTATE as
   * before.
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
                    // Drop rows the registry does not know before conversion, so the converted
                    // list stays aligned with the page. Conversion can still drop an unreadable
                    // newer-schema row; bytes are charged from the source row that produced each
                    // aspect rather than by a shared index.
                    List<EbeanAspectV2> pageAspects =
                        page.filter(
                                aspect ->
                                    handling()
                                        .admitUrn(
                                            UNKNOWN_DATA,
                                            opContext.getEntityRegistry(),
                                            opContext.getMetricUtils(),
                                            aspect.getUrn(),
                                            aspect.getAspect()))
                            .collect(Collectors.toList());

                    List<SystemAspect> systemAspects =
                        EntityUtils.toSystemAspectFromEbeanAspects(
                            opContext, opContext.getRetrieverContext(), pageAspects);

                    int sourceIndex = 0;
                    for (SystemAspect systemAspect : systemAspects) {
                      while (sourceIndex < pageAspects.size()
                          && !sameStoredRow(pageAspects.get(sourceIndex), systemAspect)) {
                        sourceIndex++;
                      }
                      if (flushBytesThreshold > 0 && sourceIndex < pageAspects.size()) {
                        tracker.bytesSinceLastFlush +=
                            metadataColumnCharLength(pageAspects.get(sourceIndex).getMetadata());
                        sourceIndex++;
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
  private static boolean sameStoredRow(EbeanAspectV2 row, SystemAspect aspect) {
    return row.getUrn().equals(aspect.getUrn().toString())
        && row.getAspect().equals(aspect.getAspectSpec().getName());
  }

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
      String aliasName,
      String oldBackingIndex,
      String nextIndex,
      long fromEpochMs,
      long toEpochMs,
      Map<String, Pair<ESIndexBuilder, ReindexConfig>> indexBuildersAndConfigs)
      throws Throwable {
    Pair<ESIndexBuilder, ReindexConfig> builderAndConfig = indexBuildersAndConfigs.get(aliasName);
    if (builderAndConfig == null) {
      throw new IllegalStateException(
          "No index builder found for timeseries index "
              + aliasName
              + "; its catch-up remains incomplete");
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
    int consecutiveIncompleteTaskLists = 0;

    while (System.currentTimeMillis() < timeoutAt) {
      Optional<TaskInfo> runningTask;
      try {
        runningTask = indexBuilder.getTaskInfoByHeader(opContext, oldBackingIndex, true);
        consecutiveIncompleteTaskLists = 0;
      } catch (IllegalStateException incompleteTaskList) {
        // A partial _tasks response cannot rule out a live reindex, and reading its shorter list as
        // an empty one here would confirm completion — disabling dual-write and releasing the
        // source index while the copy is still running. So keep polling rather than completing on
        // an answer that omits the node under stress. But this branch cannot lean on timeoutAt to
        // stop: computeTimeoutAt() is Long.MAX_VALUE under the default maxReindexHours=0, so a
        // cluster that never answers _tasks completely would hold this blocking step open forever.
        // Fail closed after a bounded run of consecutive incomplete lists instead — FAILED keeps
        // dual-write on and the source protected and surfaces the cluster problem. A single
        // trustworthy read resets the count, so a long legitimate reindex is unaffected.
        if (++consecutiveIncompleteTaskLists >= MAX_CONSECUTIVE_INCOMPLETE_TASK_LISTS) {
          log.error(
              "Timeseries catch-up for {} could not read a complete task list {} times in a row;"
                  + " failing rather than holding the upgrade open or releasing the source index."
                  + " The cluster is reporting node or task failures on _tasks: {}",
              oldBackingIndex,
              consecutiveIncompleteTaskLists,
              incompleteTaskList.getMessage());
          return CatchUpStatus.FAILED;
        }
        log.warn(
            "Timeseries catch-up task list for {} is incomplete ({}/{}); cannot confirm completion, retrying: {}",
            oldBackingIndex,
            consecutiveIncompleteTaskLists,
            MAX_CONSECUTIVE_INCOMPLETE_TASK_LISTS,
            incompleteTaskList.getMessage());
        Thread.sleep(pollIntervalMs);
        continue;
      }
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
  private void markMigrationComplete(
      String indexName, Map<String, Pair<ESIndexBuilder, ReindexConfig>> indexBuildersAndConfigs)
      throws IOException {
    Pair<ESIndexBuilder, ReindexConfig> builderAndConfig = indexBuildersAndConfigs.get(indexName);
    if (builderAndConfig == null) {
      throw new IllegalStateException(
          "No index builder found for " + indexName + "; its migration stays marked incomplete");
    }
    builderAndConfig.getFirst().markMigrationComplete(opContext, indexName);
  }

  /**
   * Resolves every index once. A name one service cannot build stays unresolved; indexes already
   * recorded, and the rest of that service, still run. An entity index fails when its migration
   * mark is written. A timeseries index fails its catch-up when the builder is missing. Either
   * failure stays non-terminal, so a re-run tries that index again.
   */
  private Map<String, Pair<ESIndexBuilder, ReindexConfig>> buildIndexBuildersAndConfigs() {
    // Read once, before the per-service handling: a failed read fails the step with its own cause
    // instead of leaving every service without configs.
    Set<Pair<Urn, StructuredPropertyDefinition>> definitions = structuredProperties.get();
    Map<String, Pair<ESIndexBuilder, ReindexConfig>> result = new LinkedHashMap<>();
    for (ElasticSearchIndexed service : indexedServices) {
      final List<ReindexConfig> configs;
      try {
        configs = service.buildReindexConfigs(opContext, definitions);
      } catch (Exception e) {
        log.warn("Error building reindex configurations", e);
        continue;
      }
      for (ReindexConfig config : configs) {
        try {
          result.putIfAbsent(
              config.name(), Pair.of(service.getIndexBuilder(config.name()), config));
        } catch (Exception e) {
          log.warn("Error resolving the index builder for {}", config.name(), e);
        }
      }
    }
    return result;
  }
}
