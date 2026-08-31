package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.getAllReindexConfigs;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder.IncrementalReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Phase 1 blocking upgrade step for incremental reindex. Creates 'next' indices with updated
 * mappings/settings, submits async ES _reindex tasks, and polls until complete. Does NOT block
 * writes on the current index or swap aliases.
 *
 * <p>Supports resumption: if the job is interrupted, on re-run it reads previously persisted state
 * and resumes polling for in-progress indices or skips already completed ones.
 *
 * <p>Persists per-index state (next index name, T0 timestamp) in a {@link DataHubUpgradeResult} for
 * Phase 2 to consume.
 */
@Slf4j
public class BuildIndicesIncrementalStep implements UpgradeStep {

  static final String UPGRADE_ID_PREFIX = IncrementalReindexState.UPGRADE_ID_PREFIX;

  private final OperationContext opContext;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final EntityService<?> entityService;
  private final String upgradeVersion;
  private final Urn upgradeIdUrn;
  private final BuildIndicesConfiguration buildIndicesConfig;

  /**
   * @param upgradeVersion version string (e.g. "{gitVersion}-{revision}") used to scope upgrade
   *     state to a specific code version. Previous state from a different version is ignored.
   */
  public BuildIndicesIncrementalStep(
      OperationContext opContext,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      EntityService<?> entityService,
      String upgradeVersion,
      BuildIndicesConfiguration buildIndicesConfig) {
    this.opContext = opContext;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.entityService = entityService;
    this.upgradeVersion = upgradeVersion;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.buildIndicesConfig = buildIndicesConfig;
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
        List<ReindexConfig> allConfigs =
            getAllReindexConfigs(context.opContext(), indexedServices, structuredProperties);
        List<ReindexConfig> configsNeedingReindex =
            allConfigs.stream()
                .filter(config -> !config.exists() || config.requiresReindex())
                .collect(Collectors.toCollection(ArrayList::new));

        if (buildIndicesConfig.isReconcileInPlaceMappingUpdates()) {
          Set<String> selectedNames =
              configsNeedingReindex.stream()
                  .map(ReindexConfig::name)
                  .collect(Collectors.toCollection(HashSet::new));
          List<ReindexConfig> reconciliationConfigs =
              allConfigs.stream()
                  .filter(ReindexConfig::requiresMappingReconciliation)
                  .filter(config -> selectedNames.add(config.name()))
                  .collect(Collectors.toList());
          configsNeedingReindex.addAll(reconciliationConfigs);
          if (!reconciliationConfigs.isEmpty()) {
            log.info(
                "Reconciling {} in-place mapping update(s) through incremental reindex: {}",
                reconciliationConfigs.size(),
                reconciliationConfigs.stream()
                    .map(ReindexConfig::name)
                    .collect(Collectors.toList()));
          }
        }

        if (configsNeedingReindex.isEmpty()) {
          log.info("No indices require incremental reindex");
        }

        // Load any previously persisted state for resumption
        Map<String, String> upgradeState = loadPreviousState(context);

        for (ReindexConfig config : configsNeedingReindex) {
          Optional<IncrementalReindexState.Status> existingStatus =
              IncrementalReindexState.getStatus(upgradeState, config.name());

          // Skip indices that already completed or were swapped in a previous run
          if (existingStatus.isPresent()
              && (existingStatus.get() == IncrementalReindexState.Status.COMPLETED
                  || existingStatus.get() == IncrementalReindexState.Status.DUAL_WRITE_DISABLED)) {
            log.info(
                "Index {} already {} in previous run, skipping",
                config.name(),
                existingStatus.get());
            continue;
          }

          ESIndexBuilder indexBuilder = findIndexBuilder(config.name());
          if (indexBuilder == null) {
            log.error("No index builder found for index: {}", config.name());
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          // Fresh-install case: index doesn't exist yet. Delegate to buildIndex, which
          // short-circuits to
          // createIndex(config.name(), config) when !exists(), and persist state as COMPLETED so
          // Phase 2 and resumed runs skip it.
          if (!config.exists()) {
            log.info("Index {} does not exist; creating directly", config.name());
            indexBuilder.buildIndex(opContext, config);
            long createTime = System.currentTimeMillis();
            upgradeState =
                IncrementalReindexState.setPhase1State(
                    upgradeState,
                    config.name(),
                    config.name(),
                    null,
                    createTime,
                    0L,
                    null,
                    false,
                    IncrementalReindexState.Status.COMPLETED);
            upgradeState =
                IncrementalReindexState.setReindexCompleteTime(
                    upgradeState, config.name(), createTime);
            checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
            continue;
          }

          boolean requiresDataBackfill = config.requiresDataBackfill();

          // Resume polling if a previous run created the next index but didn't finish polling
          Optional<String> existingNextIndex =
              IncrementalReindexState.get(
                  upgradeState, config.name(), IncrementalReindexState.NEXT_INDEX_NAME);
          if (existingStatus.isPresent()
              && existingStatus.get() == IncrementalReindexState.Status.IN_PROGRESS
              && existingNextIndex.isPresent()
              && !existingNextIndex.get().isEmpty()) {

            if (!indexBuilder.indexExists(opContext, existingNextIndex.get())) {
              log.warn(
                  "Target index {} for resume of {} no longer exists in Elasticsearch."
                      + " Resetting state and restarting reindex from scratch.",
                  existingNextIndex.get(),
                  config.name());
            } else {
              log.info(
                  "Resuming polling for index {} -> {}", config.name(), existingNextIndex.get());
              int targetShards = ESIndexBuilder.extractTargetShards(config);
              long persistedSourceDocCount =
                  IncrementalReindexState.get(
                          upgradeState, config.name(), IncrementalReindexState.SOURCE_DOC_COUNT)
                      .map(Long::parseLong)
                      .orElse(0L);
              String persistedTaskId =
                  IncrementalReindexState.get(
                          upgradeState, config.name(), IncrementalReindexState.TASK_ID)
                      .orElse("");
              // On resume, reindexInfo from the original submission is not available — use empty
              // map. Stall-retry will re-submit with fresh optimal settings if needed.
              ESIndexBuilder.PollReindexResult pollResult =
                  indexBuilder.pollReindexCompletion(
                      opContext,
                      config.name(),
                      existingNextIndex.get(),
                      () -> persistedSourceDocCount,
                      targetShards,
                      new HashMap<>(),
                      persistedTaskId);
              upgradeState =
                  handlePollResult(
                      context,
                      upgradeState,
                      config.name(),
                      existingNextIndex.get(),
                      indexBuilder,
                      pollResult.completed());
              if (!pollResult.completed()) {
                return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
              }
              // Restore settings after successful resume
              indexBuilder.undoReindexOptimalSettings(
                  opContext, existingNextIndex.get(), config, pollResult.latestReindexInfo());

              // Swap alias to next index so new code reads from the updated schema. The gate
              // compares against the source count snapshotted when this index's reindex was
              // launched, not a live alias count — see ESIndexBuilder#validateAndSwapAlias.
              if (!swapAliasOrCleanUp(
                  context,
                  upgradeState,
                  config,
                  existingNextIndex.get(),
                  persistedSourceDocCount,
                  indexBuilder,
                  "after resume")) {
                return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
              }
              // Only now is Phase 1 truly complete: mark COMPLETED so subsequent runs skip it. A
              // failed swap above returns before reaching here, having escalated the index to
              // FAILED so the next run rebuilds it.
              upgradeState =
                  IncrementalReindexState.setPhase1Completed(upgradeState, config.name());
              checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
              continue;
            }
          }

          // Fresh start for this index
          log.info("Starting incremental reindex for index: {}", config.name());

          // Resolve old backing index before creating the next one
          Set<String> oldBackingIndices = indexBuilder.getBackingIndices(opContext, config.name());
          String oldBackingIndexName =
              oldBackingIndices.size() == 1 ? oldBackingIndices.iterator().next() : null;

          IncrementalReindexResult result =
              indexBuilder.buildIndexIncremental(opContext, config, upgradeVersion);

          upgradeState =
              IncrementalReindexState.setPhase1State(
                  upgradeState,
                  config.name(),
                  result.nextIndexName(),
                  oldBackingIndexName,
                  result.reindexStartTime(),
                  result.sourceDocCount(),
                  result.taskId(),
                  requiresDataBackfill,
                  IncrementalReindexState.Status.IN_PROGRESS);
          checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);

          if (result.skippedEmpty()) {
            upgradeState =
                IncrementalReindexState.setReindexCompleteTime(
                    upgradeState, config.name(), System.currentTimeMillis());
            checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);

            // Still need to swap the alias so new code reads from the next index with correct
            // mappings, even though both indices have 0 docs.
            if (!swapAliasOrCleanUp(
                context,
                upgradeState,
                config,
                result.nextIndexName(),
                result.sourceDocCount(),
                indexBuilder,
                "empty index")) {
              return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
            }
            log.info(
                "Index {} had 0 docs, next index created as empty, alias swapped", config.name());
            upgradeState = IncrementalReindexState.setPhase1Completed(upgradeState, config.name());
            checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
            continue;
          }

          final long sourceDocCount = result.sourceDocCount();
          ESIndexBuilder.PollReindexResult pollResult =
              indexBuilder.pollReindexCompletion(
                  opContext,
                  config.name(),
                  result.nextIndexName(),
                  () -> sourceDocCount,
                  result.targetShards(),
                  result.reindexInfo(),
                  result.taskId());
          upgradeState =
              handlePollResult(
                  context,
                  upgradeState,
                  config.name(),
                  result.nextIndexName(),
                  indexBuilder,
                  pollResult.completed());
          if (!pollResult.completed()) {
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          // Restore normal index settings after successful reindex
          indexBuilder.undoReindexOptimalSettings(
              opContext, result.nextIndexName(), config, pollResult.latestReindexInfo());

          // Swap alias to next index so new code reads from the updated schema. The gate compares
          // against the source count snapshotted when the reindex was launched, not a live alias
          // count — see ESIndexBuilder#validateAndSwapAlias.
          if (!swapAliasOrCleanUp(
              context,
              upgradeState,
              config,
              result.nextIndexName(),
              sourceDocCount,
              indexBuilder,
              "after reindex")) {
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          // Only now is Phase 1 truly complete: mark COMPLETED so subsequent runs skip it. A
          // failed swap above returns before reaching here, having escalated the index to FAILED
          // so the next run rebuilds it.
          upgradeState = IncrementalReindexState.setPhase1Completed(upgradeState, config.name());
          checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
        }

        // Also handle indices that don't need reindex but need mapping/settings updates, note that
        // while we call the
        // get again it avoids reprocessing so has minimal cost.
        Set<String> incrementallyBuiltIndices =
            configsNeedingReindex.stream()
                .map(ReindexConfig::name)
                .collect(Collectors.toCollection(HashSet::new));
        List<ReindexConfig> configsNoReindex =
            allConfigs.stream()
                .filter(c -> !c.requiresReindex())
                .filter(c -> !incrementallyBuiltIndices.contains(c.name()))
                .filter(c -> c.requiresApplyMappings() || c.requiresApplySettings())
                .collect(Collectors.toList());

        for (ReindexConfig config : configsNoReindex) {
          ESIndexBuilder indexBuilder = findIndexBuilder(config.name());
          if (indexBuilder != null) {
            // Since these do not require reindexing this will just do the non-disruptive
            // settings/mappings apply
            indexBuilder.buildIndex(opContext, config);
          }
        }

        checkpoint(context, upgradeState, DataHubUpgradeState.SUCCEEDED);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Throwable e) {
        log.error("BuildIndicesIncrementalStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Handle the result of polling, updating state and checkpointing. On failure, deletes the next
   * index with retry to avoid leaving orphaned indices.
   */
  private Map<String, String> handlePollResult(
      UpgradeContext context,
      Map<String, String> upgradeState,
      String indexName,
      String nextIndexName,
      ESIndexBuilder indexBuilder,
      boolean completed) {
    if (completed) {
      // Record only the reindex (data copy) completion time and keep the status IN_PROGRESS. The
      // caller flips it to COMPLETED via IncrementalReindexState.setPhase1Completed ONLY after the
      // alias swap succeeds. This is the swap-pending intermediate state: a failed swap leaves the
      // index IN_PROGRESS so a resumed run re-polls and retries the swap, instead of hitting the
      // "already COMPLETED, skipping" branch and silently succeeding while the alias still points
      // at the stale index.
      upgradeState =
          IncrementalReindexState.setReindexCompleteTime(
              upgradeState, indexName, System.currentTimeMillis());
      checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
      log.info("Incremental reindex completed for index: {}", indexName);
    } else {
      log.error("Incremental reindex timed out for index: {}", indexName);
      failAndCleanUp(context, upgradeState, indexName, nextIndexName, indexBuilder);
    }
    return upgradeState;
  }

  /**
   * Swaps the alias onto the next index, escalating the index to {@link
   * IncrementalReindexState.Status#FAILED} if the swap does not succeed.
   *
   * <p>Both failure kinds escalate — a doc-count mismatch and an exception thrown by the swap
   * itself. Leaving the index IN_PROGRESS instead would strand it: a resumed run re-polls a target
   * it already satisfies, so the poll returns immediately without copying anything, and the only
   * action left is to re-attempt the identical swap against a destination that no longer receives
   * writes. That retry can never make progress, and because nothing transitions the status, it
   * repeats on every subsequent run. Escalating instead forces the next run down the fresh-start
   * branch, which rebuilds the index and gives the swap a destination that can actually match.
   *
   * @param phase short description of the call site, for log correlation
   * @return true if the alias was swapped; false if the index was escalated to FAILED
   */
  private boolean swapAliasOrCleanUp(
      UpgradeContext context,
      Map<String, String> upgradeState,
      ReindexConfig config,
      String nextIndexName,
      final long expectedSourceDocCount,
      ESIndexBuilder indexBuilder,
      String phase) {
    try {
      if (indexBuilder.validateAndSwapAlias(
          opContext, config.name(), nextIndexName, expectedSourceDocCount)) {
        log.info("Alias swapped: {} -> {}", config.name(), nextIndexName);
        return true;
      }
      log.error(
          "Alias swap failed for {} -> {} ({}): doc count mismatch. Marking FAILED so the next run"
              + " reindexes from scratch.",
          config.name(),
          nextIndexName,
          phase);
    } catch (Exception e) {
      log.error(
          "Alias swap failed for {} -> {} ({}). Marking FAILED so the next run reindexes from"
              + " scratch.",
          config.name(),
          nextIndexName,
          phase,
          e);
      // If the alias update succeeded and then an exception was observed (or reporting failed),
      // deleting nextIndexName would remove the live backing index. Treat "already swapped" as
      // success and skip cleanup.
      try {
        if (indexBuilder.getBackingIndices(opContext, config.name()).contains(nextIndexName)) {
          log.warn(
              "Alias {} already points to {} after swap exception; treating as success and"
                  + " skipping cleanup.",
              config.name(),
              nextIndexName);
          return true;
        }
      } catch (Exception verifyException) {
        // Verification itself failed — we cannot tell whether the alias already points at
        // nextIndexName. Mark FAILED without deleting so we never remove a potentially live
        // backing index; retention / the next fresh-start run will reclaim orphans.
        log.warn(
            "Unable to verify alias target after swap failure for {} -> {}. Marking FAILED"
                + " without cleanup to avoid deleting a potentially live backing index.",
            config.name(),
            nextIndexName,
            verifyException);
        markFailed(context, upgradeState, config.name());
        return false;
      }
    }
    failAndCleanUp(context, upgradeState, config.name(), nextIndexName, indexBuilder);
    return false;
  }

  /**
   * Marks an index FAILED and checkpoints, without deleting the next index.
   *
   * <p>FAILED is the state that guarantees recovery: it is neither skipped (only COMPLETED and
   * DUAL_WRITE_DISABLED are) nor resumed (resumption requires IN_PROGRESS), so the following run
   * takes the fresh-start branch and reindexes. Mutates {@code upgradeState} in place.
   */
  private void markFailed(
      UpgradeContext context, Map<String, String> upgradeState, String indexName) {
    upgradeState.put(
        IncrementalReindexState.key(indexName, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.FAILED.name());
    checkpoint(context, upgradeState, DataHubUpgradeState.FAILED);
  }

  /**
   * Marks an index FAILED, checkpoints, and deletes its next index.
   *
   * <p>See {@link #markFailed} for why FAILED is the recovery state. Mutates {@code upgradeState}
   * in place.
   */
  private void failAndCleanUp(
      UpgradeContext context,
      Map<String, String> upgradeState,
      String indexName,
      String nextIndexName,
      ESIndexBuilder indexBuilder) {
    markFailed(context, upgradeState, indexName);
    try {
      indexBuilder.deleteActionWithRetry(opContext, nextIndexName);
      log.info("Cleaned up failed next index: {}", nextIndexName);
    } catch (Exception e) {
      log.warn(
          "Failed to clean up next index {} (will be cleaned by retention): {}",
          nextIndexName,
          e.getMessage());
    }
  }

  private void checkpoint(
      UpgradeContext context, Map<String, String> upgradeState, DataHubUpgradeState state) {
    try {
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          opContext,
          entityService,
          upgradeIdUrn,
          DataHubUpgradeResultConditionalPersist.replaceEntireResult(upgradeState, state));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> loadPreviousState(UpgradeContext context) {
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);
    if (prevResult.isPresent() && prevResult.get().getResult() != null) {
      log.info("Loaded previous incremental reindex state for resumption");
      return new HashMap<>(prevResult.get().getResult());
    }
    return new HashMap<>();
  }

  private ESIndexBuilder findIndexBuilder(String indexName) {
    for (ElasticSearchIndexed service : indexedServices) {
      try {
        List<ReindexConfig> configs = service.buildReindexConfigs(opContext, structuredProperties);
        for (ReindexConfig config : configs) {
          if (config.name().equals(indexName)) {
            return service.getIndexBuilder();
          }
        }
      } catch (Exception e) {
        log.warn("Error checking service for index {}: {}", indexName, e.getMessage());
      }
    }
    return null;
  }
}
