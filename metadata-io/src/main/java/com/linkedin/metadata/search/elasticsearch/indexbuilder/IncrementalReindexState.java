package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

  public enum Status {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    DUAL_WRITE_DISABLED
  }

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

      String oldBackingIndexName = indexState.get(OLD_BACKING_INDEX_NAME);
      if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
        continue;
      }

      Optional<Status> phase1Status = getStatus(phase1State, indexName);
      if (phase1Status.isEmpty()) {
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
    return rollbackDualWriteEnabled
        && indexConvention != null
        && indexConvention.getEntityName(indexName).isPresent();
  }

  private static boolean hasEmptyCatchUpWindow(@Nonnull Map<String, String> indexState) {
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
        for (String prop :
            new String[] {
              NEXT_INDEX_NAME,
              OLD_BACKING_INDEX_NAME,
              REINDEX_START_TIME,
              REINDEX_COMPLETE_TIME,
              STATUS,
              DUAL_WRITE_START_TIME,
              REQUIRES_DATA_BACKFILL,
              SOURCE_DOC_COUNT,
              TASK_ID
            }) {
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
