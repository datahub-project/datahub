package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.getAllReindexConfigs;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.boot.BootstrapStep;
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
import java.util.HashMap;
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

  /**
   * @param upgradeVersion version string (e.g. "{gitVersion}-{revision}") used to scope upgrade
   *     state to a specific code version. Previous state from a different version is ignored.
   */
  public BuildIndicesIncrementalStep(
      OperationContext opContext,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      EntityService<?> entityService,
      String upgradeVersion) {
    this.opContext = opContext;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.entityService = entityService;
    this.upgradeVersion = upgradeVersion;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
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
        List<ReindexConfig> configsNeedingReindex =
            IndexUtils.getIndicesNeedingReindex(
                context.opContext(), indexedServices, structuredProperties);
        if (configsNeedingReindex.isEmpty()) {
          log.info("No indices require incremental reindex");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
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

          boolean requiresDataBackfill = config.requiresDataBackfill();

          // Resume polling if a previous run created the next index but didn't finish polling
          Optional<String> existingNextIndex =
              IncrementalReindexState.get(
                  upgradeState, config.name(), IncrementalReindexState.NEXT_INDEX_NAME);
          if (existingStatus.isPresent()
              && existingStatus.get() == IncrementalReindexState.Status.IN_PROGRESS
              && existingNextIndex.isPresent()
              && !existingNextIndex.get().isEmpty()) {
            log.info("Resuming polling for index {} -> {}", config.name(), existingNextIndex.get());
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
            // On resume, reindexInfo from the original submission is not available — use empty map.
            // Stall-retry will re-submit with fresh optimal settings if needed.
            ESIndexBuilder.PollReindexResult pollResult =
                indexBuilder.pollReindexCompletion(
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
                existingNextIndex.get(), config, pollResult.latestReindexInfo());

            // Swap alias to next index so new code reads from the updated schema
            boolean swapped =
                indexBuilder.validateAndSwapAlias(config.name(), existingNextIndex.get());
            if (!swapped) {
              log.error(
                  "Alias swap failed for {} -> {} after resume: doc count mismatch",
                  config.name(),
                  existingNextIndex.get());
              return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
            }
            log.info("Alias swapped: {} -> {}", config.name(), existingNextIndex.get());
            continue;
          }

          // Fresh start for this index
          log.info("Starting incremental reindex for index: {}", config.name());

          // Resolve old backing index before creating the next one
          Set<String> oldBackingIndices = indexBuilder.getBackingIndices(config.name());
          String oldBackingIndexName =
              oldBackingIndices.size() == 1 ? oldBackingIndices.iterator().next() : null;

          IncrementalReindexResult result =
              indexBuilder.buildIndexIncremental(config, upgradeVersion);

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
            boolean swapped =
                indexBuilder.validateAndSwapAlias(config.name(), result.nextIndexName());
            if (!swapped) {
              log.error(
                  "Alias swap failed for {} -> {} (empty index): doc count mismatch",
                  config.name(),
                  result.nextIndexName());
              return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
            }
            log.info(
                "Index {} had 0 docs, next index created as empty, alias swapped", config.name());
            continue;
          }

          final long sourceDocCount = result.sourceDocCount();
          ESIndexBuilder.PollReindexResult pollResult =
              indexBuilder.pollReindexCompletion(
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
              result.nextIndexName(), config, pollResult.latestReindexInfo());

          // Swap alias to next index so new code reads from the updated schema
          boolean swapped =
              indexBuilder.validateAndSwapAlias(config.name(), result.nextIndexName());
          if (!swapped) {
            log.error(
                "Alias swap failed for {} -> {} after reindex: doc count mismatch",
                config.name(),
                result.nextIndexName());
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          log.info("Alias swapped: {} -> {}", config.name(), result.nextIndexName());
        }

        // Also handle indices that don't need reindex but need mapping/settings updates, note that
        // while we call the
        // get again it avoids reprocessing so has minimal cost.
        List<ReindexConfig> configsNoReindex =
            getAllReindexConfigs(context.opContext(), indexedServices, structuredProperties)
                .stream()
                .filter(c -> !c.requiresReindex())
                .filter(c -> c.requiresApplyMappings() || c.requiresApplySettings())
                .collect(Collectors.toList());

        for (ReindexConfig config : configsNoReindex) {
          ESIndexBuilder indexBuilder = findIndexBuilder(config.name());
          if (indexBuilder != null) {
            // Since these do not require reindexing this will just do the non-disruptive
            // settings/mappings apply
            indexBuilder.buildIndex(config);
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
      upgradeState =
          IncrementalReindexState.setReindexCompleteTime(
              upgradeState, indexName, System.currentTimeMillis());
      checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
      log.info("Incremental reindex completed for index: {}", indexName);
    } else {
      log.error("Incremental reindex timed out for index: {}", indexName);
      upgradeState.put(
          IncrementalReindexState.key(indexName, IncrementalReindexState.STATUS),
          IncrementalReindexState.Status.FAILED.name());
      checkpoint(context, upgradeState, DataHubUpgradeState.FAILED);
      try {
        indexBuilder.deleteActionWithRetry(nextIndexName);
        log.info("Cleaned up failed next index: {}", nextIndexName);
      } catch (Exception e) {
        log.warn(
            "Failed to clean up next index {} (will be cleaned by retention): {}",
            nextIndexName,
            e.getMessage());
      }
    }
    return upgradeState;
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
