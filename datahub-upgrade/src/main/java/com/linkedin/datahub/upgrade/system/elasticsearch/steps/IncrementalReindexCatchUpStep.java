package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
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

/**
 * Phase 2 non-blocking upgrade step that closes the T0 gap created during Phase 1. Queries aspects
 * modified between {@code reindexStartTime} and {@code dualWriteStartTime} and emits MCLs via
 * Kafka. The dual-write strategy (already active in the MAE consumer) handles writing these MCLs to
 * both current and next indices.
 *
 * <p>Only processes indices where {@code requiresDataBackfill=true} — settings-only changes don't
 * need this since the ES _reindex already copied all docs correctly.
 *
 * <p>Uses {@link ChangeType#RESTATE} so the consumer re-processes the full aspect state regardless
 * of diff mode optimizations.
 */
@Slf4j
public class IncrementalReindexCatchUpStep implements UpgradeStep {

  static final String UPGRADE_ID_PREFIX = "IncrementalReindexCatchUp";
  private static final int DEFAULT_BATCH_SIZE = 500;
  public static final String LAST_URN_KEY = "lastUrn";

  private static final String TIMESERIES_TIMESTAMP_FIELD = "@timestamp";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final String upgradeVersion;
  private final boolean rollbackDualWriteEnabled;
  private final Urn upgradeIdUrn;
  private final Urn phase1UpgradeIdUrn;
  private final int batchSize;

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      String upgradeVersion,
      boolean rollbackDualWriteEnabled,
      int batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.upgradeVersion = upgradeVersion;
    this.rollbackDualWriteEnabled = rollbackDualWriteEnabled;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.phase1UpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.batchSize = batchSize;
  }

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      String upgradeVersion,
      boolean rollbackDualWriteEnabled) {
    this(
        opContext,
        entityService,
        aspectDao,
        indexedServices,
        structuredProperties,
        upgradeVersion,
        rollbackDualWriteEnabled,
        DEFAULT_BATCH_SIZE);
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

        for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
          String indexName = entry.getKey();
          Map<String, String> indexState = entry.getValue();

          String reindexStartTimeStr = indexState.get(IncrementalReindexState.REINDEX_START_TIME);
          String dualWriteStartTimeStr =
              indexState.get(IncrementalReindexState.DUAL_WRITE_START_TIME);

          if (reindexStartTimeStr == null) {
            log.warn("Index {} missing reindexStartTime, skipping catch-up", indexName);
            continue;
          }

          long reindexStartTime = Long.parseLong(reindexStartTimeStr);
          long dualWriteStartTime =
              dualWriteStartTimeStr != null
                  ? Long.parseLong(dualWriteStartTimeStr)
                  : System.currentTimeMillis();

          if (reindexStartTime >= dualWriteStartTime) {
            continue;
          }

          IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
          String nextIndexName = indexState.get(IncrementalReindexState.NEXT_INDEX_NAME);
          Optional<String> entityNameOpt = indexConvention.getEntityName(indexName);

          if (entityNameOpt.isPresent()) {
            // Entity index — emit MCLs from SQL for the gap window, scoped by entity type
            String entityName = entityNameOpt.get();
            log.info(
                "Catch-up for entity index {} (entity {}): window [{}, {}]",
                indexName,
                entityName,
                reindexStartTime,
                dualWriteStartTime);
            emitMCLsForTimeRange(
                context,
                indexName,
                "urn:li:" + entityName + ":%",
                reindexStartTime,
                dualWriteStartTime);
          } else if (indexConvention.getEntityAndAspectName(indexName).isPresent()
              && nextIndexName != null) {
            // Timeseries index — filtered _reindex from current to next for the gap window
            log.info(
                "Catch-up for timeseries index {}: _reindex window [{}, {}]",
                indexName,
                reindexStartTime,
                dualWriteStartTime);
            reindexTimeseriesGap(indexName, nextIndexName, reindexStartTime, dualWriteStartTime);
          } else if (isGlobalIndex(indexName)) {
            // Graph or system metadata index — emit MCLs for ALL entities in the gap window.
            // These indices are not entity-scoped, so we need to cover all entity types.
            // The RESTATE MCLs flow through UpdateIndicesService which updates graph, system
            // metadata, and search indices.
            log.info(
                "Catch-up for global index {}: window [{}, {}]",
                indexName,
                reindexStartTime,
                dualWriteStartTime);
            emitMCLsForTimeRange(context, indexName, "%", reindexStartTime, dualWriteStartTime);
          } else {
            log.warn(
                "Could not resolve index '{}' as entity, timeseries, or global, skipping",
                indexName);
          }
        }

        // If rollback dual-write is not enabled, mark all completed indices as
        // DUAL_WRITE_DISABLED to prevent a later enable of the flag from writing to stale or
        // deleted old indices.
        if (!rollbackDualWriteEnabled) {
          for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
            String indexName = entry.getKey();
            String status = entry.getValue().get(IncrementalReindexState.STATUS);
            if (IncrementalReindexState.Status.COMPLETED.name().equals(status)) {
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

        BootstrapStep.setUpgradeResult(opContext, upgradeIdUrn, entityService);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Throwable e) {
        log.error("IncrementalReindexCatchUpStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Streams aspects (version 0) modified in the given time range and emits RESTATE MCLs for each.
   * Uses URN-based cursor pagination with per-index checkpointing for resumption.
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

    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .batchSize(batchSize)
            .gePitEpochMs(fromEpochMs)
            .lePitEpochMs(toEpochMs)
            .urnLike(urnLikePattern)
            .lastUrn(resumeUrn)
            .urnBasedPagination(true);

    try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
      stream
          .partition(batchSize)
          .forEach(
              batch -> {
                List<Pair<Future<?>, SystemAspect>> futures =
                    EntityUtils.toSystemAspectFromEbeanAspects(
                            opContext.getRetrieverContext(), batch.collect(Collectors.toList()))
                        .stream()
                        .map(
                            systemAspect -> {
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
                              return Pair.<Future<?>, SystemAspect>of(
                                  future.getFirst(), systemAspect);
                            })
                        .toList();

                SystemAspect lastAspect =
                    futures.stream()
                        .map(
                            f -> {
                              try {
                                f.getFirst().get();
                                return f.getSecond();
                              } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                              }
                            })
                        .reduce((a, b) -> b)
                        .orElse(null);

                if (lastAspect != null) {
                  // Merge into existing state so per-index keys from other indices are preserved
                  Map<String, String> checkpoint = loadCurrentCheckpointState(context);
                  checkpoint.put(lastUrnKey, lastAspect.getUrn().toString());
                  context
                      .upgrade()
                      .setUpgradeResult(
                          opContext,
                          upgradeIdUrn,
                          entityService,
                          DataHubUpgradeState.IN_PROGRESS,
                          checkpoint);
                }
              });
    }
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
   * Copies timeseries documents from the current index to the next index for the T0 gap window
   * using a filtered ES _reindex. Timeseries data is not in SQL so MCL-based catch-up doesn't work.
   * Polls until the reindex completes and throws on failure.
   */
  private void reindexTimeseriesGap(
      String currentIndex, String nextIndex, long fromEpochMs, long toEpochMs) throws Throwable {
    Pair<ESIndexBuilder, ReindexConfig> builderAndConfig = findIndexBuilderAndConfig(currentIndex);
    if (builderAndConfig == null) {
      log.warn("No index builder found for timeseries index {}, skipping catch-up", currentIndex);
      return;
    }

    ESIndexBuilder indexBuilder = builderAndConfig.getFirst();
    ReindexConfig config = builderAndConfig.getSecond();
    int targetShards = ESIndexBuilder.extractTargetShards(config);

    RangeQueryBuilder timeRangeFilter =
        QueryBuilders.rangeQuery(TIMESERIES_TIMESTAMP_FIELD).gte(fromEpochMs).lt(toEpochMs);

    String taskId =
        indexBuilder.submitFilteredReindex(currentIndex, nextIndex, timeRangeFilter, targetShards);
    log.info(
        "Submitted timeseries catch-up _reindex for {} -> {} (task {}, shards {})",
        currentIndex,
        nextIndex,
        taskId,
        targetShards);

    ESIndexBuilder.PollReindexResult pollResult =
        indexBuilder.pollReindexCompletion(
            currentIndex, nextIndex, targetShards, new HashMap<>(), taskId);
    if (!pollResult.completed()) {
      throw new RuntimeException(
          "Timeseries catch-up _reindex did not complete for " + currentIndex + " -> " + nextIndex);
    }
    log.info("Timeseries catch-up _reindex completed for {} -> {}", currentIndex, nextIndex);
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
