package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesArgs;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.BackfillCheckpoint;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.annotation.TxIsolation;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking fill for entity indices that Phase 1 ({@link BuildIndicesIncrementalStep}) marked
 * {@link IncrementalReindexState.Status#BACKFILL_PENDING}: a new index created empty behind its
 * alias, an empty index, or a next physical index rebuilt from an index whose documents carry
 * another id scheme.
 *
 * <p>Documents come from primary storage, one entity type at a time in name order, through the same
 * loader the operator-driven LoadIndices uses. Every batch checkpoints the entity type and last URN
 * into the Phase 1 upgrade state, so an interrupted or time-budgeted run resumes where it stopped.
 *
 * <p>A run that reaches its budget ({@code maxBackfillHours}) has done what it was asked to do and
 * returns SUCCEEDED with the row still {@code BACKFILL_IN_PROGRESS}; the fill's completion is read
 * from the row, never from this step's result. The non-blocking upgrades are flattened into one
 * run, and a required step's FAILED result ends that run, so reporting the pause as a failure would
 * stop every later non-blocking step for the sake of a fill that is merely waiting for its next
 * run. A row left {@link IncrementalReindexState.Status#FAILED} is the same kind of wait, including
 * a V2 {@code _reindex} that timed out or failed its swap: this step cannot replan it, and failing
 * here would end the run before catch-up. Catch-up skips that row, replays every index that is
 * ready, and then fails the run, so the job stays red without dropping a peer's window. Phase 1
 * replans the FAILED row on the next blocking system update. Catch-up leaves a row in a fill status
 * alone and does not count it as caught up, so it is replayed once the fill completes, whatever the
 * catch-up step persisted in between.
 *
 * <p>Completeness is established as the fill runs, not counted at the end. Each page is written and
 * drained before the next is read, and a drain returns only when every document was indexed;
 * anything else fails the run. Each entity type is walked by key until a short page ends it, and a
 * resumed run re-confirms the pages it re-reads the same way, so interruptions and repeated batches
 * cannot leave a gap. A rebuild therefore swaps the alias once every type has completed and the
 * next index still carries the target mappings, with no document-count gate. The count gate the
 * {@code _reindex} path uses exists because a server-side copy reports no per-document outcome and
 * its source keeps taking writes, neither of which holds here; a count that had to tolerate re-read
 * entities would be tolerating this step's own bookkeeping, not anything about the index.
 *
 * <p>Finishing marks the row COMPLETED, the state a swapped {@code _reindex} row reaches in Phase
 * 1, so rollback dual-write and cleanup treat both alike, and records {@code backfillCompleteTime}.
 * Catch-up then replays the window since {@code backfillStartTime} and records its own status. The
 * migration mark Phase 1 set on the index stays {@code incomplete} here: catch-up sets it {@code
 * complete} once the window is replayed, so a deploy that interrupts either half finds an index it
 * must rebuild rather than one that looks finished.
 */
@Slf4j
public class BackfillEntityIndicesStep implements UpgradeStep {

  public static final String UPGRADE_ID_PREFIX = "BackfillEntityIndices";

  /** Aspect rows per loader batch when {@code backfillBatchSize} is unset or not positive. */
  public static final int DEFAULT_BATCH_SIZE = 5000;

  /**
   * Batches between cursor writes. A batch is one synchronous write round trip, so persisting every
   * one would scale upgrade-aspect writes with fill throughput; the pending cursor is flushed when
   * an entity type's run ends, normally or not.
   */
  /** Rows written between cursor checkpoints: the most a resume re-processes. */
  static final int CHECKPOINT_EVERY_ROWS = 10_000;

  /** Below this many entities of the largest type per page, per-page costs dominate. */
  static final int PAGE_SIZE_ENTITY_MULTIPLE = 10;

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final String upgradeVersion;
  private final BuildIndicesConfiguration buildIndicesConfig;
  private final AspectIndexLoader loader;
  private final UpdateIndicesService updateIndicesService;
  private final Urn phase1UpgradeIdUrn;

  public BackfillEntityIndicesStep(
      OperationContext opContext,
      EntityService<?> entityService,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      String upgradeVersion,
      @Nullable BuildIndicesConfiguration buildIndicesConfig,
      AspectIndexLoader loader,
      UpdateIndicesService updateIndicesService) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.upgradeVersion = upgradeVersion;
    this.buildIndicesConfig =
        buildIndicesConfig != null ? buildIndicesConfig : new BuildIndicesConfiguration();
    this.loader = loader;
    this.updateIndicesService = updateIndicesService;
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
        Map<String, String> phase1State = loadPhase1State(context);
        List<String> pending = pendingFills(phase1State);
        if (pending.isEmpty()) {
          // A FAILED row, fill or V2 _reindex, is not pending. Catch-up replays the indices that
          // are ready and then fails the run; failing here would end the run before that replay.
          log.info("No entity indices await a fill");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }
        validatePageSize();
        // Records each index's builder alongside its config; both are needed below.
        List<ReindexConfig> configs =
            IndexUtils.getAllReindexConfigs(opContext, indexedServices, structuredProperties);
        Long deadlineEpochMs = deadlineEpochMs();
        for (String indexName : pending) {
          phase1State = fillIndex(context, phase1State, configs, indexName, deadlineEpochMs);
        }
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (AspectIndexLoader.DeadlineExceeded e) {
        // Not a failure: the budget bounds one run, the cursor is checkpointed (fillIndex flushes
        // it on the way out), and the row stays in a fill status for the next run to resume. See
        // the class comment for why this must not fail the non-blocking run.
        final String paused =
            String.format(
                "Fill budget of %d hour(s) reached after %d aspects (last urn %s); the fill stays in"
                    + " progress and the next non-blocking run resumes from the checkpoint",
                buildIndicesConfig.getMaxBackfillHours(), e.rowsProcessed(), e.lastUrn());
        log.warn(paused);
        context.report().addLine(paused);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Throwable e) {
        log.error("BackfillEntityIndicesStep failed; checkpoints are kept for resume", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /** Rows still in a fill status, in a stable order so resumes take them the same way. */
  private static List<String> pendingFills(@Nonnull Map<String, String> phase1State) {
    return IncrementalReindexState.getAllIndexStates(phase1State).keySet().stream()
        .filter(
            indexName ->
                IncrementalReindexState.isBackfillStatus(
                    IncrementalReindexState.getStatus(phase1State, indexName).orElse(null)))
        .sorted()
        .collect(Collectors.toList());
  }

  private Map<String, String> fillIndex(
      UpgradeContext context,
      Map<String, String> phase1State,
      List<ReindexConfig> configs,
      String indexName,
      @Nullable Long deadlineEpochMs)
      throws Exception {
    final Map<String, String> row =
        IncrementalReindexState.getAllIndexStates(phase1State).get(indexName);
    final String target = row.get(IncrementalReindexState.BACKFILL_TARGET_INDEX);
    if (target == null || target.isEmpty()) {
      throw new IllegalStateException(
          "Index " + indexName + " awaits a fill but records no fill target");
    }
    final IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final String indexKey =
        indexConvention
            .getEntityIndexKeyV3(opContext, indexName)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Index " + indexName + " awaits a fill but is not a V3 entity index"));
    final Collection<EntitySpec> keyedEntitySpecs =
        V3IndexKeys.entitySpecsForKey(opContext.getEntityRegistry(), indexKey);
    if (keyedEntitySpecs.isEmpty()) {
      throw new IllegalStateException(
          "No entity types resolve to index key '" + indexKey + "' for " + indexName);
    }
    // Only entity types with searchable aspects produce a V3 document; the transformer yields
    // nothing for the others, so streaming them is wasted work. An index whose types produce no
    // document at all (a system entity type with no searchable aspect, under a layout that gives
    // it an index of its own) has nothing to fill and completes as the empty fill it is.
    final List<EntitySpec> entitySpecs =
        keyedEntitySpecs.stream()
            .filter(BackfillEntityIndicesStep::producesSearchDocuments)
            .sorted(Comparator.comparing(EntitySpec::getName))
            .collect(Collectors.toList());
    final ESIndexBuilder indexBuilder = IndexUtils.requireIndexBuilder(indexName);
    final boolean rebuild = !target.equals(indexName);
    final ReindexConfig config =
        configs.stream()
            .filter(candidate -> candidate.name().equals(indexName))
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException("No reindex config for index " + indexName));

    if (rebuild) {
      // The next index may have been reaped or recreated by dynamic mapping since Phase 1 made it.
      // Filling a destination without the target mappings is wasted work that also poisons field
      // types, so fail this index instead; FAILED is neither skipped nor resumed by Phase 1, which
      // creates a fresh next index on the next run.
      requireTargetMappings(indexBuilder, config, indexName, target);
    }

    if (IncrementalReindexState.getStatus(phase1State, indexName).orElse(null)
        == IncrementalReindexState.Status.BACKFILL_PENDING) {
      phase1State =
          persistPhase1(
              phase1State,
              state ->
                  withStatus(
                      state, indexName, IncrementalReindexState.Status.BACKFILL_IN_PROGRESS));
    }

    final Optional<BackfillCheckpoint> checkpoint =
        IncrementalReindexState.getBackfillCheckpoint(phase1State, indexName);
    long rowsSoFar = checkpoint.map(BackfillCheckpoint::processedCount).orElse(0L);
    long urnsSoFar =
        IncrementalReindexState.getBackfillUrnsProcessed(phase1State, indexName).orElse(0L);
    context
        .report()
        .addLine(
            String.format(
                "Filling %s into %s (key %s, %d entity type(s))%s",
                indexName,
                target,
                indexKey,
                entitySpecs.size(),
                checkpoint
                    .map(c -> ", resuming at " + c.cursorEntity() + " / " + c.cursorUrn())
                    .orElse("")));

    for (int i = 0; i < entitySpecs.size(); i++) {
      final String entityName = entitySpecs.get(i).getName();
      if (checkpoint.isPresent() && entityName.compareTo(checkpoint.get().cursorEntity()) < 0) {
        continue;
      }
      final String lastUrn =
          checkpoint
              .filter(c -> entityName.equals(c.cursorEntity()))
              .map(BackfillCheckpoint::cursorUrn)
              .orElse(null);
      final FillCheckpointer checkpointer =
          new FillCheckpointer(indexName, entityName, rowsSoFar, urnsSoFar);
      final LoadIndicesResult result;
      try {
        result =
            loader.load(
                opContext,
                fillArgs(entityName, lastUrn, target, deadlineEpochMs),
                message -> {
                  log.info("Fill {} [{}]: {}", indexName, entityName, message);
                  return null;
                },
                checkpointer::onBatch,
                (ctx, batch) -> {
                  requireTarget(batch, target);
                  updateIndicesService.fillSearchIndices(ctx, batch);
                });
      } catch (RuntimeException e) {
        try {
          checkpointer.flush();
        } catch (RuntimeException flushFailure) {
          e.addSuppressed(flushFailure);
        }
        throw e;
      }
      rowsSoFar += result.rowsProcessed;
      urnsSoFar += result.urnsProcessed;
      // Point the cursor at the next entity type with no URN so a resume starts it from the
      // beginning instead of repeating this one.
      final String nextEntity =
          i + 1 < entitySpecs.size() ? entitySpecs.get(i + 1).getName() : entityName;
      final long rows = rowsSoFar;
      final long urns = urnsSoFar;
      phase1State =
          persistPhase1(
              phase1State, state -> checkpoint(state, indexName, nextEntity, null, rows, urns));
    }

    if (rebuild) {
      // Completeness was established page by page as the fill ran (see the class comment), so what
      // remains to verify is the destination itself: a next index deleted mid-fill is recreated by
      // dynamic mapping under the first bulk write that follows, which a document count would take
      // for a populated index and this check does not.
      requireTargetMappings(indexBuilder, config, indexName, target);
      phase1State = retainConcreteLiveIndex(phase1State, indexBuilder, config, indexName);
      indexBuilder.swapAlias(opContext, indexName, target);
      log.info("Alias swapped after fill: {} -> {}", indexName, target);
      if (IncrementalReindexState.isOldBackingIndexIdSchemeStale(phase1State, indexName)) {
        log.info(
            "Index {} was rebuilt under a new document-id scheme: live writes are not mirrored into"
                + " {}, so a rollback to it returns to its pre-swap content",
            indexName,
            IncrementalReindexState.get(
                    phase1State, indexName, IncrementalReindexState.OLD_BACKING_INDEX_NAME)
                .orElse("its old backing index"));
      } else if (IncrementalReindexState.isOldBackingIndexIncompleteMigration(
          phase1State, indexName)) {
        log.info(
            "Index {} was rebuilt from an abandoned migration: live writes are not mirrored into"
                + " {}, so a rollback to it returns to its pre-swap content",
            indexName,
            IncrementalReindexState.get(
                    phase1State, indexName, IncrementalReindexState.OLD_BACKING_INDEX_NAME)
                .orElse("its old backing index"));
      }
    }
    final long completeTime = System.currentTimeMillis();
    phase1State =
        persistPhase1(
            phase1State,
            state ->
                IncrementalReindexState.setPhase1Completed(
                    IncrementalReindexState.setBackfillCompleteTime(state, indexName, completeTime),
                    indexName));
    context
        .report()
        .addLine(
            String.format(
                "Filled %s: %d aspects, %d entities read%s",
                indexName, rowsSoFar, urnsSoFar, rebuild ? ", alias swapped to " + target : ""));
    return phase1State;
  }

  /**
   * A V3 index created before indices were aliased owns the convention name itself. The alias needs
   * that name and nothing renames an index in place, so the swap has to delete it; the index is
   * first copied to a physical name of its family and that copy recorded as the old backing index,
   * so the alias can be swapped back to it. An aliased index has its old backing index recorded by
   * Phase 1 and is left alone.
   *
   * <p>The row names the copy as soon as it exists. A run that dies while the copy is in flight
   * leaves a row naming it and, possibly, a reindex still writing into it; the next run leaves that
   * copy alone, since deleting an index under a live writer only has the writer recreate it, and
   * takes a fresh one. The abandoned copy is a physical index of the family that nothing refers to,
   * which a later cleanup reaps. A copy that fails has its reindex cancelled and confirmed stopped
   * before it is deleted; when that cannot be confirmed the copy is left for cleanup too. Either
   * way the row fails, so Phase 1 plans the rebuild again.
   */
  private Map<String, String> retainConcreteLiveIndex(
      Map<String, String> phase1State,
      ESIndexBuilder indexBuilder,
      ReindexConfig config,
      String indexName)
      throws Exception {
    if (!indexBuilder.getBackingIndices(opContext, indexName).isEmpty()) {
      return phase1State;
    }
    IncrementalReindexState.get(
            phase1State, indexName, IncrementalReindexState.OLD_BACKING_INDEX_NAME)
        .ifPresent(
            previousCopy ->
                log.info(
                    "Index {}: a previous run left the copy {}; leaving it for cleanup and copying"
                        + " afresh",
                    indexName,
                    previousCopy));
    log.info(
        "Index {} owns its name directly; copying it before the alias takes the name", indexName);
    ESIndexBuilder.IncrementalReindexResult copy =
        indexBuilder.startRollbackCopy(opContext, config, upgradeVersion);
    final String retained = copy.nextIndexName();
    Map<String, String> named =
        persistPhase1(
            phase1State,
            state -> IncrementalReindexState.setOldBackingIndexName(state, indexName, retained));
    try {
      indexBuilder.finishRollbackCopy(opContext, config, copy);
    } catch (Exception e) {
      log.error("Index {}: copy into {} failed; failing the row", indexName, retained, e);
      try {
        indexBuilder.cancelRollbackCopy(opContext, copy);
        indexBuilder.deleteActionWithRetry(opContext, retained);
      } catch (Exception cleanupFailure) {
        log.warn(
            "Index {}: leaving the copy {} in place for cleanup",
            indexName,
            retained,
            cleanupFailure);
        e.addSuppressed(cleanupFailure);
      }
      persistPhase1(
          named,
          state ->
              withStatus(
                  withoutOldBackingIndex(state, indexName),
                  indexName,
                  IncrementalReindexState.Status.FAILED));
      throw e;
    }
    return named;
  }

  private static Map<String, String> withoutOldBackingIndex(
      Map<String, String> state, String indexName) {
    Map<String, String> updated = new HashMap<>(state);
    updated.remove(
        IncrementalReindexState.key(indexName, IncrementalReindexState.OLD_BACKING_INDEX_NAME));
    return updated;
  }

  /**
   * Fails the row unless {@code target} still exists with the mappings this fill was planned for.
   * FAILED is neither skipped nor resumed by Phase 1, which creates a fresh next index.
   */
  private void requireTargetMappings(
      ESIndexBuilder indexBuilder, ReindexConfig config, String indexName, String target)
      throws Exception {
    if (indexBuilder.resumedReindexMatchesTarget(opContext, config, target)) {
      return;
    }
    persistPhase1(
        Map.of(), state -> withStatus(state, indexName, IncrementalReindexState.Status.FAILED));
    throw new IllegalStateException(
        "Fill target "
            + target
            + " for "
            + indexName
            + " is missing or no longer carries the target mappings; marked FAILED so Phase 1"
            + " rebuilds it");
  }

  /**
   * Every event this step hands to the strategies must name the fill target. The header is how the
   * strategy is told to write a rebuild into the next index rather than the alias, and without it
   * the strategy writes the alias, as the operator LoadIndices command intends. A loader that
   * stopped stamping it would therefore fill the live index in silence, so the contract is checked
   * here, where the events were requested, rather than inferred from a count afterwards.
   */
  private static void requireTarget(
      @Nonnull final List<MetadataChangeLog> batch, @Nonnull final String target) {
    for (MetadataChangeLog event : batch) {
      final String stamped =
          event.getHeaders() == null
              ? null
              : event.getHeaders().get(BackfillHeaders.TARGET_INDEX_HEADER_NAME);
      if (!target.equals(stamped)) {
        throw new IllegalStateException(
            String.format(
                "Fill event for %s names target '%s' but this fill writes %s",
                event.getEntityUrn(), stamped, target));
      }
    }
  }

  private LoadIndicesArgs fillArgs(
      String entityName, @Nullable String lastUrn, String target, @Nullable Long deadlineEpochMs) {
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = pageSize();
    args.limit = Integer.MAX_VALUE;
    args.urnLike = "urn:li:" + entityName + ":%";
    args.lastUrn = lastUrn;
    // No aspect filter: the stream then carries every stored aspect of each entity, the loader
    // stamps the events as complete entities, and the V3 writer builds documents from them without
    // reading the same rows back from primary storage.
    args.v3BackfillMode = BackfillHeaders.MODE_BASELINE;
    args.v3TargetIndex = target;
    args.deadlineEpochMs = deadlineEpochMs;
    args.txIsolation = TxIsolation.READ_COMMITED;
    return args;
  }

  private int pageSize() {
    return buildIndicesConfig.getBackfillBatchSize() > 0
        ? buildIndicesConfig.getBackfillBatchSize()
        : DEFAULT_BATCH_SIZE;
  }

  /**
   * A page must hold at least one whole entity, or no entity can be emitted complete and every one
   * falls back to hydration. Well below {@link #PAGE_SIZE_ENTITY_MULTIPLE} entities per page the
   * per-page query, drain and checkpoint are paid for little work, so that is warned about.
   */
  private void validatePageSize() {
    final int pageSize = pageSize();
    final int largestEntity = largestEntityAspectCount(opContext.getEntityRegistry());
    if (pageSize < largestEntity) {
      throw new IllegalStateException(
          String.format(
              "elasticsearch.buildIndices.backfillBatchSize=%d cannot hold one whole entity: the"
                  + " largest entity type has %d stored aspects",
              pageSize, largestEntity));
    }
    if (pageSize < PAGE_SIZE_ENTITY_MULTIPLE * largestEntity) {
      log.warn(
          "elasticsearch.buildIndices.backfillBatchSize={} holds fewer than {} entities of the"
              + " largest type ({} stored aspects); each fill page pays its query, drain and"
              + " checkpoint for little work",
          pageSize,
          PAGE_SIZE_ENTITY_MULTIPLE,
          largestEntity);
    }
  }

  /** Same rule as the search document transformer: no searchable aspects, no document. */
  static boolean producesSearchDocuments(@Nonnull final EntitySpec entitySpec) {
    return !entitySpec.getSearchableFieldSpecs().isEmpty();
  }

  /** Stored (non-timeseries) aspects of the entity type that has the most of them. */
  static int largestEntityAspectCount(@Nonnull final EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs().values().stream()
        .mapToInt(
            spec -> (int) spec.getAspectSpecs().stream().filter(a -> !a.isTimeseries()).count())
        .max()
        .orElse(0);
  }

  @Nullable
  private Long deadlineEpochMs() {
    int hours = buildIndicesConfig.getMaxBackfillHours();
    return hours > 0 ? System.currentTimeMillis() + hours * 3_600_000L : null;
  }

  private static Map<String, String> checkpoint(
      Map<String, String> state,
      String indexName,
      String cursorEntity,
      @Nullable String cursorUrn,
      long rowsProcessed,
      long urnsProcessed) {
    Map<String, String> updated =
        IncrementalReindexState.setBackfillCheckpoint(
            state, indexName, new BackfillCheckpoint(cursorEntity, cursorUrn, rowsProcessed, 0L));
    return IncrementalReindexState.setBackfillUrnsProcessed(updated, indexName, urnsProcessed);
  }

  private static Map<String, String> withStatus(
      Map<String, String> state, String indexName, IncrementalReindexState.Status status) {
    Map<String, String> updated = new HashMap<>(state);
    updated.put(
        IncrementalReindexState.key(indexName, IncrementalReindexState.STATUS), status.name());
    return updated;
  }

  /** Cursor of one entity type's loader run, persisted every few batches and on the way out. */
  private final class FillCheckpointer {
    private final String indexName;
    private final String entityName;
    private final long rowsBase;
    private final long urnsBase;
    private long rowsInRunAtLastFlush;
    @Nullable private String pendingUrn;
    private long pendingRows;
    private long pendingUrns;

    FillCheckpointer(String indexName, String entityName, long rowsBase, long urnsBase) {
      this.indexName = indexName;
      this.entityName = entityName;
      this.rowsBase = rowsBase;
      this.urnsBase = urnsBase;
    }

    void onBatch(String lastUrn, long rowsInRun, long urnsInRun) {
      pendingUrn = lastUrn;
      pendingRows = rowsBase + rowsInRun;
      pendingUrns = urnsBase + urnsInRun;
      if (rowsInRun - rowsInRunAtLastFlush >= CHECKPOINT_EVERY_ROWS) {
        flush();
      }
    }

    void flush() {
      if (pendingUrn == null) {
        return;
      }
      final String urn = pendingUrn;
      final long rows = pendingRows;
      final long urns = pendingUrns;
      try {
        persistPhase1(Map.of(), state -> checkpoint(state, indexName, entityName, urn, rows, urns));
      } catch (Exception e) {
        // The batches have landed but their cursor has not; failing the run keeps the previous
        // checkpoint, from which a resume rewrites them.
        throw new IllegalStateException("Failed to checkpoint fill progress for " + indexName, e);
      }
      rowsInRunAtLastFlush = rows - rowsBase;
      pendingUrn = null;
    }
  }

  private Map<String, String> loadPhase1State(UpgradeContext context) {
    return context
        .upgrade()
        .getUpgradeResult(opContext, phase1UpgradeIdUrn, entityService)
        .map(DataHubUpgradeResult::getResult)
        .<Map<String, String>>map(HashMap::new)
        .orElseGet(HashMap::new);
  }

  /**
   * Applies {@code mutation} to the stored Phase 1 state under optimistic locking and returns it
   * applied to {@code current}, the caller's view. Mutations are pure map transforms, so both
   * copies agree.
   */
  private Map<String, String> persistPhase1(
      Map<String, String> current, UnaryOperator<Map<String, String>> mutation) throws Exception {
    DataHubUpgradeResultConditionalPersist.mergeAndPersist(
        opContext,
        entityService,
        phase1UpgradeIdUrn,
        (map, existingState) -> {
          Map<String, String> updated = mutation.apply(new HashMap<>(map));
          map.clear();
          map.putAll(updated);
          return existingState != null ? existingState : DataHubUpgradeState.IN_PROGRESS;
        });
    return mutation.apply(current);
  }
}
