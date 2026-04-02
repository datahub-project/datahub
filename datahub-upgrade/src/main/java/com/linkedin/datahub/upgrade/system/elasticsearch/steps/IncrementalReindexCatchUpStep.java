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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

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

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final String upgradeVersion;
  private final Urn upgradeIdUrn;
  private final Urn phase1UpgradeIdUrn;
  private final int batchSize;

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      String upgradeVersion,
      int batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.upgradeVersion = upgradeVersion;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.phase1UpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            BuildIndicesIncrementalStep.UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.batchSize = batchSize;
  }

  public IncrementalReindexCatchUpStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      String upgradeVersion) {
    this(opContext, entityService, aspectDao, upgradeVersion, DEFAULT_BATCH_SIZE);
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

          if (!Boolean.parseBoolean(
              indexState.get(IncrementalReindexState.REQUIRES_DATA_BACKFILL))) {
            continue;
          }

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

          Optional<String> entityNameOpt =
              opContext.getSearchContext().getIndexConvention().getEntityName(indexName);
          if (entityNameOpt.isEmpty()) {
            log.warn("Could not resolve entity name for index '{}', skipping catch-up", indexName);
            continue;
          }

          String entityName = entityNameOpt.get();
          log.info(
              "Catch-up for index {} (entity {}): window [{}, {}]",
              indexName,
              entityName,
              reindexStartTime,
              dualWriteStartTime);

          emitMCLsForTimeRange(
              context, indexName, entityName, reindexStartTime, dualWriteStartTime);
        }

        BootstrapStep.setUpgradeResult(opContext, upgradeIdUrn, entityService);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("IncrementalReindexCatchUpStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Streams aspects (version 0) for a specific entity type modified in the given time range and
   * emits RESTATE MCLs for each. Uses URN-based cursor pagination with per-index checkpointing for
   * resumption.
   *
   * @param indexName the ES index name, used as a prefix for per-index resume state
   * @param entityName the entity type name, used to scope the DB query via urnLike
   */
  private void emitMCLsForTimeRange(
      UpgradeContext context,
      String indexName,
      String entityName,
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
            .urnLike("urn:li:" + entityName + ":%")
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
}
