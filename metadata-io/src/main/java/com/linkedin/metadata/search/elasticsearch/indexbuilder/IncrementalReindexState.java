package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Manages per-index state for incremental reindex upgrades, stored in {@link
 * DataHubUpgradeResult}'s flat {@code Map<String, String>} result field.
 *
 * <p>Keys are prefixed by the index name (e.g. {@code "datasetindex_v2.nextIndexName"}) so multiple
 * indices can be tracked in a single upgrade result.
 */
public final class IncrementalReindexState {

  public static final String UPGRADE_ID_PREFIX = "BuildIndicesIncremental";
  public static final String CATCH_UP_UPGRADE_ID_PREFIX = "IncrementalReindexCatchUp";

  private IncrementalReindexState() {}

  private static final String SEPARATOR = ".";

  /** Physical name of the next index created during Phase 1. */
  public static final String NEXT_INDEX_NAME = "nextIndexName";

  /** Physical name of the old backing index that the alias pointed to before the Phase 1 swap. */
  public static final String OLD_BACKING_INDEX_NAME = "oldBackingIndexName";

  /**
   * Set when the index was rebuilt because its documents were written under another document-id
   * scheme: the old backing index stores each entity under an id the running hasher does not
   * produce, so live writes cannot be mirrored into it and a rollback to it returns to its pre-swap
   * content.
   */
  public static final String OLD_BACKING_INDEX_ID_SCHEME_STALE = "oldBackingIndexIdSchemeStale";

  /**
   * Set when the index was rebuilt because an earlier deploy's migration of it never finished. The
   * old backing index is not a generation to roll back to: mirroring new writes into it would not
   * fill the gap, so live writes stay out of it and a rollback returns to its pre-swap content.
   */
  public static final String OLD_BACKING_INDEX_INCOMPLETE_MIGRATION =
      "oldBackingIndexIncompleteMigration";

  /** Epoch millis when the ES _reindex task was submitted (T0). */
  public static final String REINDEX_START_TIME = "reindexStartTime";

  /** Epoch millis when the ES _reindex task completed. */
  public static final String REINDEX_COMPLETE_TIME = "reindexCompleteTime";

  /** Current status of this index's incremental reindex. */
  public static final String STATUS = "status";

  /**
   * Epoch millis when the MAE consumer first performed a dual-write to the next index. Set at
   * runtime by the upgrade strategy, not during Phase 1.
   */
  public static final String DUAL_WRITE_START_TIME = "dualWriteStartTime";

  /** Whether this index requires a data backfill (new @Searchable fields, not just settings). */
  public static final String REQUIRES_DATA_BACKFILL = "requiresDataBackfill";

  /** Source index document count snapshotted at _reindex submission time. */
  public static final String SOURCE_DOC_COUNT = "sourceDocCount";

  /** ES task ID for the _reindex task submitted during Phase 1. */
  public static final String TASK_ID = "taskId";

  /** Per-index Phase 2 catch-up completion status, stored on the catch-up upgrade result. */
  public static final String CATCH_UP_STATUS = "catchUpStatus";

  /** Resume cursor property. Keys are {@code index.lastUrn} and {@code index.entity.lastUrn}. */
  public static final String CATCH_UP_LAST_URN = "lastUrn";

  /**
   * Entity index generation of this row. Absent on rows written before generations were tracked,
   * which were all V2 because Phase 1 refused V3 indices.
   */
  public static final String INDEX_VERSION = "indexVersion";

  /** Index the historical fill writes into: the alias, or the next physical index for a rebuild. */
  public static final String BACKFILL_TARGET_INDEX = "backfillTargetIndex";

  /** Entity type the fill was scanning when it last checkpointed. */
  public static final String BACKFILL_CURSOR_ENTITY = "backfillCursorEntity";

  /** Last URN fully written for the cursor entity; the resume point is the URN after it. */
  public static final String BACKFILL_CURSOR_URN = "backfillCursorUrn";

  public static final String BACKFILL_PROCESSED_COUNT = "backfillProcessedCount";

  public static final String BACKFILL_TOTAL_ESTIMATE = "backfillTotalEstimate";

  /**
   * Baseline document generation pinned when the fill started. A resumed run must write with the
   * same value so a half-filled index never carries two baselines racing each other.
   */
  public static final String BACKFILL_GENERATION = "backfillGeneration";

  /** Epoch millis when the fill first started scanning; the catch-up window opens here. */
  public static final String BACKFILL_START_TIME = "backfillStartTime";

  /** Epoch millis when the fill finished scanning. */
  public static final String BACKFILL_COMPLETE_TIME = "backfillCompleteTime";

  /**
   * Entities the fill has read so far, summed over its runs. A resumed run re-reads the entity it
   * checkpointed at, so this is progress reporting, not a document count; nothing is gated on it.
   */
  public static final String BACKFILL_URNS_PROCESSED = "backfillUrnsProcessed";

  private static final List<String> TRACKED_PROPERTIES =
      List.of(
          NEXT_INDEX_NAME,
          OLD_BACKING_INDEX_NAME,
          OLD_BACKING_INDEX_ID_SCHEME_STALE,
          OLD_BACKING_INDEX_INCOMPLETE_MIGRATION,
          REINDEX_START_TIME,
          REINDEX_COMPLETE_TIME,
          STATUS,
          DUAL_WRITE_START_TIME,
          REQUIRES_DATA_BACKFILL,
          SOURCE_DOC_COUNT,
          TASK_ID,
          INDEX_VERSION,
          BACKFILL_TARGET_INDEX,
          BACKFILL_CURSOR_ENTITY,
          BACKFILL_CURSOR_URN,
          BACKFILL_PROCESSED_COUNT,
          BACKFILL_TOTAL_ESTIMATE,
          BACKFILL_GENERATION,
          BACKFILL_START_TIME,
          BACKFILL_COMPLETE_TIME,
          BACKFILL_URNS_PROCESSED);

  public enum Status {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    DUAL_WRITE_DISABLED,
    BACKFILL_PENDING,
    BACKFILL_IN_PROGRESS
  }

  /** Resume point for a historical fill, persisted between batches. */
  public record BackfillCheckpoint(
      @Nonnull String cursorEntity,
      @Nullable String cursorUrn,
      long processedCount,
      long totalEstimate) {}

  public enum CatchUpStatus {
    PENDING,
    COMPLETED,
    SKIPPED,
    FAILED;

    public boolean isTerminal() {
      return this == COMPLETED || this == SKIPPED;
    }
  }

  /** Build a prefixed key for a given index. */
  public static String key(@Nonnull String indexName, @Nonnull String property) {
    return indexName + SEPARATOR + property;
  }

  /** Read a property for a given index from the result map. */
  public static Optional<String> get(
      @Nullable Map<String, String> resultMap,
      @Nonnull String indexName,
      @Nonnull String property) {
    if (resultMap == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(resultMap.get(key(indexName, property)));
  }

  /** Read the status for a given index. */
  public static Optional<Status> getStatus(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, STATUS).map(Status::valueOf);
  }

  /** Read the Phase 2 catch-up status for a given index. */
  public static Optional<CatchUpStatus> getCatchUpStatus(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, CATCH_UP_STATUS).map(CatchUpStatus::valueOf);
  }

  /** Write Phase 2 catch-up status for an index into the result map. */
  public static Map<String, String> setCatchUpStatus(
      @Nullable Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull CatchUpStatus status) {
    Map<String, String> result = existing != null ? new HashMap<>(existing) : new HashMap<>();
    result.put(key(indexName, CATCH_UP_STATUS), status.name());
    return result;
  }

  /**
   * Drop a catch-up verdict and that index's resume cursors, so a later fill of the same index is
   * replayed from the start of its own window. Cursors are {@code index.lastUrn} and {@code
   * index.entity.lastUrn}.
   */
  public static Map<String, String> clearCatchUpStatus(
      @Nullable Map<String, String> existing, @Nonnull String indexName) {
    Map<String, String> result = existing != null ? new HashMap<>(existing) : new HashMap<>();
    result.remove(key(indexName, CATCH_UP_STATUS));
    removeResumeCursors(result, indexName);
    return result;
  }

  /**
   * {@code updated} merged onto {@code loaded}. A catch-up status {@code updated} no longer carries
   * is removed, and so are that index's resume cursors, so neither is copied back from the previous
   * result. A cursor for an index whose status {@code updated} still carries is left in place.
   */
  public static Map<String, String> mergeCatchUpState(
      @Nullable Map<String, String> loaded, @Nonnull Map<String, String> updated) {
    Map<String, String> merged = loaded != null ? new HashMap<>(loaded) : new HashMap<>();
    merged.putAll(updated);
    String statusSuffix = SEPARATOR + CATCH_UP_STATUS;
    List<String> clearedIndices = new ArrayList<>();
    for (String key : new ArrayList<>(merged.keySet())) {
      if (key.endsWith(statusSuffix) && !updated.containsKey(key)) {
        merged.remove(key);
        clearedIndices.add(key.substring(0, key.length() - statusSuffix.length()));
      }
    }
    clearedIndices.forEach(indexName -> removeResumeCursors(merged, indexName));
    // A cursor whose index no longer has a catch-up status was cleared from the in-memory map.
    // Drop it here too, so the previous result cannot copy it back. A cursor for an index whose
    // status is still carried stays, even when this map does not repeat the key.
    String cursorSuffix = SEPARATOR + CATCH_UP_LAST_URN;
    merged
        .keySet()
        .removeIf(
            key ->
                key.endsWith(cursorSuffix)
                    && !updated.containsKey(key)
                    && !updated.containsKey(
                        key(indexOwningResumeCursor(key, updated), CATCH_UP_STATUS)));
    return merged;
  }

  /**
   * Index that owns a resume cursor key. The key is {@code index.lastUrn} or {@code
   * index.entity.lastUrn}. An index name may itself contain the separator, so a status still
   * carried for the longer name wins.
   */
  private static String indexOwningResumeCursor(
      @Nonnull String cursorKey, @Nonnull Map<String, String> updated) {
    String body =
        cursorKey.substring(0, cursorKey.length() - (SEPARATOR + CATCH_UP_LAST_URN).length());
    int dot = body.lastIndexOf(SEPARATOR);
    if (dot < 0 || updated.containsKey(key(body, CATCH_UP_STATUS))) {
      return body;
    }
    return body.substring(0, dot);
  }

  /** Resume cursors for one index: {@code index.lastUrn} and {@code index.<entity>.lastUrn}. */
  private static void removeResumeCursors(
      @Nonnull Map<String, String> state, @Nonnull String indexName) {
    String prefix = indexName + SEPARATOR;
    String cursorSuffix = SEPARATOR + CATCH_UP_LAST_URN;
    state.keySet().removeIf(key -> key.startsWith(prefix) && key.endsWith(cursorSuffix));
  }

  /** Read the entity index generation for an index; rows predating tracking are V2. */
  @Nonnull
  public static EntityIndexVersion getIndexVersion(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, INDEX_VERSION)
        .map(EntityIndexVersion::valueOf)
        .orElse(EntityIndexVersion.V2);
  }

  /** Marks {@code indexName}'s old backing index as holding documents under another id scheme. */
  public static Map<String, String> setOldBackingIndexIdSchemeStale(
      @Nonnull Map<String, String> existing, @Nonnull String indexName) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, OLD_BACKING_INDEX_ID_SCHEME_STALE), Boolean.TRUE.toString());
    return result;
  }

  /** Whether live writes must stay out of {@code indexName}'s old backing index; see the key. */
  public static boolean isOldBackingIndexIdSchemeStale(
      @Nullable Map<String, String> state, @Nonnull String indexName) {
    return state != null
        && Boolean.parseBoolean(state.get(key(indexName, OLD_BACKING_INDEX_ID_SCHEME_STALE)));
  }

  /** Marks {@code indexName}'s old backing index as an abandoned migration, not a rollback. */
  public static Map<String, String> setOldBackingIndexIncompleteMigration(
      @Nonnull Map<String, String> existing, @Nonnull String indexName) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, OLD_BACKING_INDEX_INCOMPLETE_MIGRATION), Boolean.TRUE.toString());
    return result;
  }

  /** Whether {@code indexName}'s old backing index is an abandoned migration; see the key. */
  public static boolean isOldBackingIndexIncompleteMigration(
      @Nullable Map<String, String> state, @Nonnull String indexName) {
    return state != null
        && Boolean.parseBoolean(state.get(key(indexName, OLD_BACKING_INDEX_INCOMPLETE_MIGRATION)));
  }

  /** Records the physical index the alias of {@code indexName} can be swapped back to. */
  public static Map<String, String> setOldBackingIndexName(
      @Nonnull Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull String oldBackingIndexName) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, OLD_BACKING_INDEX_NAME), oldBackingIndexName);
    return result;
  }

  public static Map<String, String> setIndexVersion(
      @Nonnull Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull EntityIndexVersion version) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, INDEX_VERSION), version.name());
    return result;
  }

  /**
   * Mark an index as awaiting a historical fill into {@code targetIndex}: the index's own alias for
   * a fresh or empty index, or a next physical index for a rebuild that is swapped in once filled.
   * The baseline generation is pinned separately by the fill when it starts, see {@link
   * #setBackfillGeneration}.
   */
  public static Map<String, String> setBackfillPending(
      @Nullable Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull String targetIndex,
      long startTime) {
    Map<String, String> result = existing != null ? new HashMap<>(existing) : new HashMap<>();
    // A fill planned afresh starts from nothing: the progress and verdicts of an earlier attempt
    // for this index, re-planned after it failed, must not carry over, or the new fill would resume
    // at the old cursor and skip what came before it.
    for (String property : FRESH_FILL_PROPERTIES) {
      result.remove(key(indexName, property));
    }
    result.put(key(indexName, BACKFILL_TARGET_INDEX), targetIndex);
    result.put(key(indexName, BACKFILL_START_TIME), String.valueOf(startTime));
    result.put(key(indexName, STATUS), Status.BACKFILL_PENDING.name());
    return result;
  }

  /** Properties of an earlier fill attempt that a fill planned afresh does not inherit. */
  private static final List<String> FRESH_FILL_PROPERTIES =
      List.of(
          BACKFILL_CURSOR_ENTITY,
          BACKFILL_CURSOR_URN,
          BACKFILL_PROCESSED_COUNT,
          BACKFILL_TOTAL_ESTIMATE,
          BACKFILL_URNS_PROCESSED,
          BACKFILL_GENERATION,
          BACKFILL_COMPLETE_TIME,
          OLD_BACKING_INDEX_ID_SCHEME_STALE,
          OLD_BACKING_INDEX_INCOMPLETE_MIGRATION);

  /** Pin the baseline generation every document of this fill is written with. */
  public static Map<String, String> setBackfillGeneration(
      @Nonnull Map<String, String> existing, @Nonnull String indexName, long generation) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, BACKFILL_GENERATION), String.valueOf(generation));
    return result;
  }

  public static Optional<Long> getBackfillGeneration(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, BACKFILL_GENERATION).map(Long::parseLong);
  }

  public static Map<String, String> setBackfillUrnsProcessed(
      @Nonnull Map<String, String> existing, @Nonnull String indexName, long urnsProcessed) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, BACKFILL_URNS_PROCESSED), String.valueOf(urnsProcessed));
    return result;
  }

  public static Optional<Long> getBackfillUrnsProcessed(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, BACKFILL_URNS_PROCESSED).map(Long::parseLong);
  }

  public static boolean isBackfillStatus(@Nullable Status status) {
    return status == Status.BACKFILL_PENDING || status == Status.BACKFILL_IN_PROGRESS;
  }

  /** Record fill progress and mark the fill in progress. */
  public static Map<String, String> setBackfillCheckpoint(
      @Nonnull Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull BackfillCheckpoint checkpoint) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, BACKFILL_CURSOR_ENTITY), checkpoint.cursorEntity());
    if (checkpoint.cursorUrn() != null) {
      result.put(key(indexName, BACKFILL_CURSOR_URN), checkpoint.cursorUrn());
    } else {
      result.remove(key(indexName, BACKFILL_CURSOR_URN));
    }
    result.put(
        key(indexName, BACKFILL_PROCESSED_COUNT), String.valueOf(checkpoint.processedCount()));
    result.put(key(indexName, BACKFILL_TOTAL_ESTIMATE), String.valueOf(checkpoint.totalEstimate()));
    result.put(key(indexName, STATUS), Status.BACKFILL_IN_PROGRESS.name());
    return result;
  }

  public static Optional<BackfillCheckpoint> getBackfillCheckpoint(
      @Nullable Map<String, String> resultMap, @Nonnull String indexName) {
    return get(resultMap, indexName, BACKFILL_CURSOR_ENTITY)
        .map(
            cursorEntity ->
                new BackfillCheckpoint(
                    cursorEntity,
                    get(resultMap, indexName, BACKFILL_CURSOR_URN).orElse(null),
                    get(resultMap, indexName, BACKFILL_PROCESSED_COUNT)
                        .map(Long::parseLong)
                        .orElse(0L),
                    get(resultMap, indexName, BACKFILL_TOTAL_ESTIMATE)
                        .map(Long::parseLong)
                        .orElse(0L)));
  }

  /**
   * Record when the fill finished scanning. Like {@link #setReindexCompleteTime}, this leaves the
   * status alone: the index becomes {@link Status#COMPLETED} only once the swap (for rebuilds) and
   * catch-up have succeeded.
   */
  public static Map<String, String> setBackfillCompleteTime(
      @Nonnull Map<String, String> existing, @Nonnull String indexName, long completeTime) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, BACKFILL_COMPLETE_TIME), String.valueOf(completeTime));
    return result;
  }

  /**
   * Returns physical index names that must not be deleted by orphan cleanup while incremental
   * reindex catch-up or rollback dual-write still depends on them.
   *
   * <p>Timeseries indices are released once catch-up reaches a terminal status. Entity search
   * indices with {@code rollbackDualWriteEnabled=true} stay protected until Phase 1 status is
   * {@link Status#DUAL_WRITE_DISABLED}, because the MAE consumer may still dual-write to the old
   * backing index for rollback safety after catch-up completes.
   */
  @Nonnull
  public static Set<String> getProtectedPhysicalIndicesForCleanup(
      @Nullable Map<String, String> phase1State,
      @Nullable Map<String, String> catchUpState,
      @Nullable IndexConvention indexConvention,
      boolean rollbackDualWriteEnabled) {
    if (phase1State == null || phase1State.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> protectedIndices = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : getAllIndexStates(phase1State).entrySet()) {
      String indexName = entry.getKey();
      Map<String, String> indexState = entry.getValue();

      Optional<Status> phase1Status = getStatus(phase1State, indexName);
      if (phase1Status.isEmpty()) {
        continue;
      }
      // A fill in flight owns everything it touches: the physical index it writes (still without an
      // alias for a rebuild, so it reads as an orphan) and the old backing index it will swap away
      // from. Neither may be reaped until the fill completes and the swap runs.
      if (isBackfillStatus(phase1Status.get())) {
        protectedIndices.addAll(fillProtectedIndices(indexName, indexState));
        continue;
      }

      String oldBackingIndexName = indexState.get(OLD_BACKING_INDEX_NAME);
      if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
        continue;
      }
      if (phase1Status.get() == Status.DUAL_WRITE_DISABLED) {
        continue;
      }
      if (phase1Status.get() != Status.COMPLETED && phase1Status.get() != Status.IN_PROGRESS) {
        continue;
      }

      if (hasEmptyCatchUpWindow(indexState)) {
        continue;
      }

      Optional<CatchUpStatus> catchUpStatus = getCatchUpStatus(catchUpState, indexName);
      if (catchUpStatus.isPresent() && catchUpStatus.get().isTerminal()) {
        if (!requiresRollbackDualWriteProtection(
            indexName, indexConvention, rollbackDualWriteEnabled)) {
          continue;
        }
      }

      protectedIndices.add(oldBackingIndexName);
    }

    return protectedIndices;
  }

  private static Set<String> fillProtectedIndices(
      @Nonnull String indexName, @Nonnull Map<String, String> indexState) {
    Set<String> protectedIndices = new HashSet<>();
    for (String property :
        List.of(BACKFILL_TARGET_INDEX, NEXT_INDEX_NAME, OLD_BACKING_INDEX_NAME)) {
      String physicalIndex = indexState.get(property);
      if (physicalIndex != null && !physicalIndex.isEmpty() && !physicalIndex.equals(indexName)) {
        protectedIndices.add(physicalIndex);
      }
    }
    return protectedIndices;
  }

  /** Returns true when every index with a T0 gap has terminal catch-up status. */
  public static boolean isCatchUpCompleteForAllIndices(
      @Nullable Map<String, String> phase1State, @Nullable Map<String, String> catchUpState) {
    if (phase1State == null || phase1State.isEmpty()) {
      return true;
    }
    for (Map.Entry<String, Map<String, String>> entry : getAllIndexStates(phase1State).entrySet()) {
      if (hasEmptyCatchUpWindow(entry.getValue())) {
        continue;
      }
      Optional<CatchUpStatus> catchUpStatus = getCatchUpStatus(catchUpState, entry.getKey());
      if (catchUpStatus.isEmpty() || !catchUpStatus.get().isTerminal()) {
        return false;
      }
    }
    return true;
  }

  private static boolean requiresRollbackDualWriteProtection(
      @Nonnull String indexName,
      @Nullable IndexConvention indexConvention,
      boolean rollbackDualWriteEnabled) {
    // Any entity index generation qualifies, not only V2: a V3 old backing index is dual-written
    // after a swap exactly like a V2 one.
    return rollbackDualWriteEnabled
        && indexConvention != null
        && indexConvention.entityIndexVersionOf(indexName).isPresent();
  }

  /**
   * When an index's catch-up window opens: the fill start for a filled index (writes handled by
   * pre-upgrade consumers during the rollout reached only the old generation), otherwise the
   * reindex start. Empty for rows that record neither.
   */
  public static Optional<Long> catchUpWindowStart(@Nonnull Map<String, String> indexState) {
    String start = indexState.get(BACKFILL_START_TIME);
    if (start == null) {
      start = indexState.get(REINDEX_START_TIME);
    }
    return Optional.ofNullable(start).map(Long::parseLong);
  }

  private static boolean hasEmptyCatchUpWindow(@Nonnull Map<String, String> indexState) {
    // A filled index always has a window: writes handled by pre-upgrade consumers during the
    // rollout land only on the old generation, regardless of when this generation's dual-write
    // began. The window is closed by catch-up, never by timing.
    if (indexState.containsKey(BACKFILL_START_TIME)) {
      return false;
    }
    String reindexStartTimeStr = indexState.get(REINDEX_START_TIME);
    if (reindexStartTimeStr == null) {
      return true;
    }
    long reindexStartTime = Long.parseLong(reindexStartTimeStr);
    String dualWriteStartTimeStr = indexState.get(DUAL_WRITE_START_TIME);
    long dualWriteStartTime =
        dualWriteStartTimeStr != null ? Long.parseLong(dualWriteStartTimeStr) : Long.MAX_VALUE;
    return reindexStartTime >= dualWriteStartTime;
  }

  /** Write Phase 1 state for an index into the result map. */
  public static Map<String, String> setPhase1State(
      @Nullable Map<String, String> existing,
      @Nonnull String indexName,
      @Nonnull String nextIndexName,
      @Nullable String oldBackingIndexName,
      long reindexStartTime,
      long sourceDocCount,
      @Nullable String taskId,
      boolean requiresDataBackfill,
      @Nonnull Status status) {

    Map<String, String> result = existing != null ? new HashMap<>(existing) : new HashMap<>();
    result.put(key(indexName, NEXT_INDEX_NAME), nextIndexName);
    if (oldBackingIndexName != null) {
      result.put(key(indexName, OLD_BACKING_INDEX_NAME), oldBackingIndexName);
    }
    result.put(key(indexName, REINDEX_START_TIME), String.valueOf(reindexStartTime));
    result.put(key(indexName, SOURCE_DOC_COUNT), String.valueOf(sourceDocCount));
    if (taskId != null) {
      result.put(key(indexName, TASK_ID), taskId);
    }
    result.put(key(indexName, REQUIRES_DATA_BACKFILL), String.valueOf(requiresDataBackfill));
    result.put(key(indexName, STATUS), status.name());
    return result;
  }

  /**
   * Record Phase 1 reindex (data copy) completion time. Deliberately does NOT change the status:
   * the index stays {@link Status#IN_PROGRESS} until the alias swap actually succeeds, at which
   * point {@link #setPhase1Completed} flips it to {@link Status#COMPLETED}. Marking COMPLETED here
   * (before the swap) makes a failed swap unrecoverable — a resumed run would hit the "already
   * COMPLETED, skipping" branch and silently succeed while the alias still points at the stale
   * index, instead of retrying the swap.
   */
  public static Map<String, String> setReindexCompleteTime(
      @Nonnull Map<String, String> existing, @Nonnull String indexName, long completeTime) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, REINDEX_COMPLETE_TIME), String.valueOf(completeTime));
    return result;
  }

  /**
   * Mark Phase 1 fully complete for an index. Must be called ONLY after the alias swap has
   * succeeded. A failed swap must leave the index {@link Status#IN_PROGRESS} so a resumed run
   * re-polls and retries the swap rather than skipping the index.
   */
  public static Map<String, String> setPhase1Completed(
      @Nonnull Map<String, String> existing, @Nonnull String indexName) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, STATUS), Status.COMPLETED.name());
    return result;
  }

  /** Record when dual-write started for this index. */
  public static Map<String, String> setDualWriteStartTime(
      @Nonnull Map<String, String> existing, @Nonnull String indexName, long startTime) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, DUAL_WRITE_START_TIME), String.valueOf(startTime));
    return result;
  }

  /** Mark an index as having dual-write disabled (rollback no longer needed or not enabled). */
  public static Map<String, String> setDualWriteDisabled(
      @Nonnull Map<String, String> existing, @Nonnull String indexName) {
    Map<String, String> result = new HashMap<>(existing);
    result.put(key(indexName, STATUS), Status.DUAL_WRITE_DISABLED.name());
    return result;
  }

  /**
   * Merge for Phase 1 {@link DataHubUpgradeResult} after disabling dual-write for {@code
   * indexName}.
   */
  @Nonnull
  public static DataHubUpgradeResultConditionalPersist.Merge persistDualWriteDisabledMerge(
      @Nonnull String indexName, @Nullable DataHubUpgradeState phaseState) {
    return (map, existingState) -> {
      Map<String, String> updated = setDualWriteDisabled(new HashMap<>(map), indexName);
      map.clear();
      map.putAll(updated);
      return existingState != null ? existingState : phaseState;
    };
  }

  /**
   * Extract all index names that have state tracked in the result map by finding keys ending in the
   * status suffix.
   */
  public static Map<String, Map<String, String>> getAllIndexStates(
      @Nullable Map<String, String> resultMap) {
    Map<String, Map<String, String>> indexStates = new HashMap<>();
    if (resultMap == null) {
      return indexStates;
    }
    String statusSuffix = SEPARATOR + STATUS;
    for (Map.Entry<String, String> entry : resultMap.entrySet()) {
      if (entry.getKey().endsWith(statusSuffix)) {
        String indexName =
            entry.getKey().substring(0, entry.getKey().length() - statusSuffix.length());
        Map<String, String> state = new HashMap<>();
        for (String prop : TRACKED_PROPERTIES) {
          String val = resultMap.get(key(indexName, prop));
          if (val != null) {
            state.put(prop, val);
          }
        }
        indexStates.put(indexName, state);
      }
    }
    return indexStates;
  }
}
