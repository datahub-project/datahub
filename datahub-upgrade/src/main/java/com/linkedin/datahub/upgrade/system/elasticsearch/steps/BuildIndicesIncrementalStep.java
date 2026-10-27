package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.getAllReindexConfigs;
import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.requireIndexBuilder;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentProjector.URN_FIELD;

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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexMigrationStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
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

  /**
   * Documents read to judge whether an index that records no document-id scheme carries the running
   * hasher's ids. One request; the index is rebuilt either way.
   */
  private static final int ID_SCHEME_SAMPLE_SIZE = 100;

  private final OperationContext opContext;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final EntityService<?> entityService;
  private final String upgradeVersion;
  private final Urn upgradeIdUrn;
  private final BuildIndicesConfiguration buildIndicesConfig;
  private final UpdateIndicesService updateIndicesService;
  private final V3DocumentIdScheme docIdScheme;
  private final boolean entityIndexFillAvailable;

  /**
   * @param upgradeVersion version string (e.g. "{gitVersion}-{revision}") used to scope upgrade
   *     state to a specific code version. Previous state from a different version is ignored.
   * @param updateIndicesService source of which indices are filled from primary storage rather than
   *     created empty
   * @param docIdScheme decides whether an existing filled index holds documents from another id
   *     scheme and must be rebuilt
   * @param entityIndexFillAvailable whether this deployment can run the non-blocking fill that
   *     consumes the fill rows this step would plan; false refuses instead of planning them
   */
  public BuildIndicesIncrementalStep(
      OperationContext opContext,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      EntityService<?> entityService,
      String upgradeVersion,
      BuildIndicesConfiguration buildIndicesConfig,
      UpdateIndicesService updateIndicesService,
      V3DocumentIdScheme docIdScheme,
      boolean entityIndexFillAvailable) {
    this.opContext = opContext;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.entityService = entityService;
    this.upgradeVersion = upgradeVersion;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.buildIndicesConfig = buildIndicesConfig;
    this.updateIndicesService = updateIndicesService;
    this.docIdScheme = docIdScheme;
    this.entityIndexFillAvailable = entityIndexFillAvailable;
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
        Set<String> filledIndices = updateIndicesService.indicesFilledFromPrimaryStorage(opContext);
        // Load any previously persisted state for resumption
        Map<String, String> upgradeState = loadPreviousState(context);
        Map<String, Fill> fills = decideFills(allConfigs, filledIndices, upgradeState);
        if (!fills.isEmpty() && !entityIndexFillAvailable) {
          // A fill row with no filler is a silent stall: the alias points at an empty or stale
          // index, catch-up skips it, and it never completes. Refuse before creating anything.
          log.error(
              "Indices {} need a fill from primary storage, but this deployment cannot stream it"
                  + " (no aspect index loader; fills require entityService.impl=ebean)",
              fills.keySet());
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        List<ReindexConfig> configsNeedingReindex =
            allConfigs.stream()
                .filter(
                    config ->
                        !config.exists()
                            || config.requiresReindex()
                            || fills.containsKey(config.name()))
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

        for (ReindexConfig config : configsNeedingReindex) {
          Optional<IncrementalReindexState.Status> existingStatus =
              IncrementalReindexState.getStatus(upgradeState, config.name());

          // Skip indices that already completed or were swapped in a previous run, and indices
          // whose fill the non-blocking phase owns: the fill resumes from its own checkpoint.
          if (ownedByThisDeploy(upgradeState, config.name())) {
            log.info(
                "Index {} already {} in previous run, skipping",
                config.name(),
                existingStatus.get());
            continue;
          }

          ESIndexBuilder indexBuilder = requireIndexBuilder(config.name());

          Fill fill = fills.get(config.name());
          if (fill != null) {
            upgradeState = startFill(context, upgradeState, config, fill, indexBuilder);
            continue;
          }

          // Fresh-install case for indices that are not filled from primary storage: the index
          // doesn't exist yet. Delegate to buildIndex, which short-circuits to
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
                IncrementalReindexState.setIndexVersion(
                    upgradeState, config.name(), indexVersionOf(config.name()));
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
          upgradeState =
              IncrementalReindexState.setIndexVersion(
                  upgradeState, config.name(), indexVersionOf(config.name()));
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
          ESIndexBuilder indexBuilder = requireIndexBuilder(config.name());
          // Since these do not require reindexing this will just do the non-disruptive
          // settings/mappings apply.
          indexBuilder.buildIndex(opContext, config);
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

  /** How an index filled from primary storage receives its documents this run. */
  enum Fill {
    /** The index does not exist: create it empty behind its alias; the fill writes to the alias. */
    CREATE_ALIASED,
    /**
     * The index exists, holds no documents, and already has the target mappings and settings: the
     * fill writes into it in place.
     */
    IN_PLACE,
    /**
     * The index exists with documents from another id scheme, holds no documents and differs from
     * the target mappings or settings, or holds a partial corpus from a fill that a later deploy
     * can no longer see: create a next physical index for the fill to write into; the alias moves
     * to it once the fill completes.
     */
    REBUILD_INTO_NEXT
  }

  /**
   * Whether this deploy's state already accounts for {@code indexName}: the row is finished, or a
   * fill the non-blocking phase owns is in flight. Such a row is never planned again; a FAILED row
   * is, and an index with no row at all is judged from what the index itself shows.
   */
  private static boolean ownedByThisDeploy(Map<String, String> upgradeState, String indexName) {
    return IncrementalReindexState.getStatus(upgradeState, indexName)
        .map(
            status ->
                status == IncrementalReindexState.Status.COMPLETED
                    || status == IncrementalReindexState.Status.DUAL_WRITE_DISABLED
                    || IncrementalReindexState.isBackfillStatus(status))
        .orElse(false);
  }

  /**
   * Decided before any index is touched. An existing index that needs a rebuild is classified from
   * its live mapping, which must happen before a stamped mapping could be applied to it (see {@link
   * V3DocumentIdScheme}).
   */
  private Map<String, Fill> decideFills(
      List<ReindexConfig> allConfigs, Set<String> filledIndices, Map<String, String> upgradeState)
      throws IOException {
    Map<String, Fill> fills = new HashMap<>();
    for (ReindexConfig config : allConfigs) {
      if (!filledIndices.contains(config.name())) {
        continue;
      }
      if (!config.exists()) {
        fills.put(config.name(), Fill.CREATE_ALIASED);
      } else if (docIdScheme.requiresDocumentRebuild(config.currentMappings())) {
        fills.put(config.name(), Fill.REBUILD_INTO_NEXT);
      } else if (requireIndexBuilder(config.name()).getCount(opContext, config.name()) == 0) {
        // A finished migration of an entity type nobody has ingested. Planning it again would
        // mark the index incomplete, and live writes during that refill would then look like an
        // abandoned partial corpus.
        if (IndexMigrationStamp.complete(config.currentMappings()) && matchesTarget(config)) {
          continue;
        }
        // Empty is the steady state of an entity type nobody has ingested. Filled where it
        // stands, the index would receive documents under a mapping Phase 1 still has to change,
        // and the in-place apply pass below never reaches an index with a fill row.
        fills.put(config.name(), matchesTarget(config) ? Fill.IN_PLACE : Fill.REBUILD_INTO_NEXT);
      } else if (IndexMigrationStamp.incomplete(config.currentMappings())) {
        // The alias holds documents written by a migration whose rows belong to an earlier deploy,
        // and either its fill or the catch-up that follows it never finished. The index matches
        // the target, so neither a mapping reindex nor an empty-index fill would run, and what is
        // missing would never be filled or replayed. Rebuild it. A row of this deploy means this
        // run is a repeat and the row says where that migration stands; it is not planned again.
        if (ownedByThisDeploy(upgradeState, config.name())) {
          log.info(
              "Index {}: migration still in progress under this deploy's own row", config.name());
          continue;
        }
        log.warn(
            "Index {}: an earlier deploy left its migration unfinished; rebuilding into a fresh"
                + " index",
            config.name());
        fills.put(config.name(), Fill.REBUILD_INTO_NEXT);
      }
    }
    if (!fills.isEmpty()) {
      log.info("Indices to fill from primary storage: {}", fills);
    }
    return fills;
  }

  /**
   * Whether the index a rebuild leaves behind holds its documents under ids the running hasher does
   * not produce, so that no live write may be mirrored into it after the swap: a mirrored write
   * would sit beside the old copy and a mirrored delete would miss it. A recorded scheme answers by
   * itself. An index that records none is judged from a random sample of its documents, which reads
   * an index built under the running hasher before stamping existed as current and lets rollback
   * writes continue; the sample's blind spot is described on {@link
   * V3DocumentIdScheme#countIdsFromAnotherScheme}. An index whose sample cannot be read is treated
   * as stale, since nothing shows that writes into it would address its documents.
   */
  private boolean oldIndexIdSchemeStale(ReindexConfig config, ESIndexBuilder indexBuilder) {
    Map<String, Object> liveMappings = config.currentMappings();
    if (!docIdScheme.requiresDocumentRebuild(liveMappings)) {
      return false;
    }
    if (V3DocumentIdScheme.stampedOn(liveMappings).isPresent()) {
      return true;
    }
    final Map<String, String> sample;
    try {
      sample =
          indexBuilder.sampleDocumentFieldById(
              opContext, config.name(), URN_FIELD, ID_SCHEME_SAMPLE_SIZE);
    } catch (Exception e) {
      log.warn(
          "Index {}: could not sample document ids; treating its documents as written under"
              + " another document-id scheme, so the index the rebuild leaves behind receives no"
              + " rollback writes after the swap",
          config.name(),
          e);
      return true;
    }
    if (sample.isEmpty()) {
      log.info(
          "Index {}: no document-id scheme recorded and no documents to sample; rebuilt, and"
              + " rollback writes continue after the swap",
          config.name());
      return false;
    }
    long fromAnotherScheme = docIdScheme.countIdsFromAnotherScheme(opContext, sample);
    if (fromAnotherScheme > 0) {
      log.warn(
          "Index {}: {} of {} sampled documents carry ids from another document-id scheme; rebuilt"
              + " into a fresh index, and the index the rebuild leaves behind receives no rollback"
              + " writes after the swap",
          config.name(),
          fromAnotherScheme,
          sample.size());
      return true;
    }
    log.info(
        "Index {}: no document-id scheme recorded, but all {} sampled documents carry the running"
            + " scheme's ids; rebuilt once to record it, and rollback writes continue after the"
            + " swap",
        config.name(),
        sample.size());
    return false;
  }

  /** Whether an existing index already carries the target mappings and settings. */
  private static boolean matchesTarget(ReindexConfig config) {
    return !config.requiresReindex()
        && !config.requiresApplyMappings()
        && !config.requiresApplySettings();
  }

  /**
   * Creates whatever the fill writes into and records the index as {@link
   * IncrementalReindexState.Status#BACKFILL_PENDING}. The fill itself, the swap for rebuilds, and
   * the catch-up run in the non-blocking phase.
   */
  private Map<String, String> startFill(
      UpgradeContext context,
      Map<String, String> upgradeState,
      ReindexConfig config,
      Fill fill,
      ESIndexBuilder indexBuilder)
      throws IOException {
    final long startTime = System.currentTimeMillis();
    final String physicalIndex;
    final String fillTarget;
    String oldBackingIndexName = null;
    boolean oldIdSchemeStale = false;
    boolean abandonedMigration = false;
    switch (fill) {
      case CREATE_ALIASED -> {
        physicalIndex =
            ESIndexBuilder.getIncrementalNextIndexName(config.name(), upgradeVersion, startTime);
        indexBuilder.createAliasedIndex(opContext, config.name(), physicalIndex, config);
        indexBuilder.markMigrationIncomplete(opContext, physicalIndex);
        fillTarget = config.name();
      }
      case REBUILD_INTO_NEXT -> {
        // Whether live writes may be mirrored into the index the swap leaves behind turns on its
        // ids, not on the rebuild; the row records the answer for the consumers. A partial corpus
        // left by an abandoned migration is not a generation to roll back to either.
        oldIdSchemeStale = oldIndexIdSchemeStale(config, indexBuilder);
        abandonedMigration = IndexMigrationStamp.incomplete(config.currentMappings());
        Set<String> oldBackingIndices = indexBuilder.getBackingIndices(opContext, config.name());
        oldBackingIndexName =
            oldBackingIndices.size() == 1 ? oldBackingIndices.iterator().next() : null;
        physicalIndex =
            ESIndexBuilder.getIncrementalNextIndexName(config.name(), upgradeVersion, startTime);
        indexBuilder.createIndex(opContext, physicalIndex, config);
        indexBuilder.markMigrationIncomplete(opContext, physicalIndex);
        fillTarget = physicalIndex;
      }
      case IN_PLACE -> {
        physicalIndex = config.name();
        fillTarget = config.name();
        indexBuilder.markMigrationIncomplete(opContext, config.name());
      }
      default -> throw new IllegalStateException("Unhandled fill " + fill);
    }
    log.info(
        "Index {}: {}; fill target {} (physical index {})",
        config.name(),
        fill,
        fillTarget,
        physicalIndex);

    upgradeState =
        IncrementalReindexState.setPhase1State(
            upgradeState,
            config.name(),
            physicalIndex,
            oldBackingIndexName,
            startTime,
            0L,
            null,
            false,
            IncrementalReindexState.Status.BACKFILL_PENDING);
    upgradeState =
        IncrementalReindexState.setBackfillPending(
            upgradeState, config.name(), fillTarget, startTime);
    upgradeState =
        IncrementalReindexState.setIndexVersion(
            upgradeState, config.name(), indexVersionOf(config.name()));
    if (oldIdSchemeStale) {
      upgradeState =
          IncrementalReindexState.setOldBackingIndexIdSchemeStale(upgradeState, config.name());
    }
    if (abandonedMigration) {
      log.info(
          "Index {}: rebuilt from an abandoned migration; the index the rebuild leaves behind"
              + " receives no rollback writes",
          config.name());
      upgradeState =
          IncrementalReindexState.setOldBackingIndexIncompleteMigration(
              upgradeState, config.name());
    }
    checkpoint(context, upgradeState, DataHubUpgradeState.IN_PROGRESS);
    return upgradeState;
  }

  private EntityIndexVersion indexVersionOf(String indexName) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .entityIndexVersionOf(indexName)
        .orElse(EntityIndexVersion.V2);
  }
}
