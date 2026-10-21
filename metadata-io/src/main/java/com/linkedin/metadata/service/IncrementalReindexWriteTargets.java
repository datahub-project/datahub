package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Physical indices other than the alias that a live write must also reach while an incremental
 * reindex is under way, read from the Phase 1 state the upgrade persists.
 *
 * <p><b>Rollback targets.</b> Phase 1 records, per swapped index, the physical index its alias used
 * to point at. Until Phase 2 marks that index {@link
 * IncrementalReindexState.Status#DUAL_WRITE_DISABLED}, every live write to the alias is also
 * applied to the old backing index so the alias can be swapped back. Targets are keyed by the
 * swapped index's alias, the same key Phase 1 state uses, so the V2 and V3 update strategies share
 * one source and each resolves its target from the index it just wrote to. Rollback targets are
 * loaded only when rollback dual-write is enabled. An index rebuilt because its documents were
 * written under another document-id scheme, as its stamp or a sample of its ids showed, has no
 * rollback target: the running hasher's ids would store a second copy of each entity next to the
 * old one, so its old backing index keeps its pre-swap content and a rollback to it returns to
 * that. Nor does an index rebuilt from the partial corpus an earlier deploy's migration left: that
 * index was never a generation to roll back to, and mirroring writes into it would not fill its
 * gap.
 *
 * <p><b>Fill targets.</b> A fill that rebuilds an index writes a next physical index while the
 * alias keeps serving the old one, and catch-up re-derives its window from primary storage, where a
 * hard delete leaves nothing to re-derive. So while such a fill is in flight, a live delete is also
 * applied to the index the fill is building; otherwise the deleted entity would come back with the
 * swap. Fill targets are tracked whether or not rollback dual-write is enabled.
 *
 * <p>Loaded once at startup and kept current by polling the persisted upgrade state for the life of
 * the process: an index swapped after startup (a fill rebuilt in the non-blocking phase) is added,
 * an index marked DUAL_WRITE_DISABLED is removed, and a fill's target is dropped once the fill has
 * swapped it in. A state that holds nothing means nothing is in flight right now, not that nothing
 * will be: consumers can be serving before the upgrade has written any state for their version. The
 * poll runs often until it has read that state and while a fill is in flight, since a fill both
 * starts and swaps while the consumers are serving, and rarely otherwise. A fill re-planned for the
 * same version while the consumers idle is seen on the next rare poll. Closing stops the poller;
 * the targets held so far stay readable.
 */
@Slf4j
public class IncrementalReindexWriteTargets implements AutoCloseable {

  private static final long IDLE_POLL_INTERVAL_SECONDS = 300;

  /**
   * Until the state for this version has been read, so a fill planned after startup is mirrored
   * from its first pages, and while a fill is in flight, so its swapped-out index becomes a
   * rollback target before much live traffic passes it by.
   */
  private static final long WATCHFUL_POLL_INTERVAL_SECONDS = 15;

  /** Persists {@code dualWriteStartTime} for the swapped index on its first rollback write. */
  @FunctionalInterface
  public interface DualWriteStartTimeCallback {
    void onDualWriteStarted(@Nonnull String indexName, long startTimeMillis);
  }

  /** Where the Phase 1 upgrade result is read from, for the initial load and the poller. */
  public record StateSource(
      @Nonnull OperationContext opContext,
      @Nonnull DataHubUpgradeResultStore upgradeResultStore,
      @Nonnull Urn upgradeIdUrn) {}

  public record RollbackTarget(
      @Nonnull String indexName,
      @Nonnull String oldBackingIndexName,
      @Nonnull EntityIndexVersion indexVersion) {}

  private final ConcurrentHashMap<String, RollbackTarget> rollbackTargets;
  private final ConcurrentHashMap<String, String> fillTargets;
  private final ConcurrentHashMap<String, AtomicBoolean> dualWriteStartRecorded;
  @Nullable private final DualWriteStartTimeCallback dualWriteStartTimeCallback;
  @Nullable private final StateSource stateSource;
  private final boolean rollbackDualWriteEnabled;
  @Nullable private final ScheduledExecutorService statePoller;
  private volatile boolean phase1StateRead;

  public IncrementalReindexWriteTargets(
      @Nonnull final Map<String, RollbackTarget> initialRollbackTargets,
      @Nonnull final Map<String, String> initialFillTargets,
      @Nullable final DualWriteStartTimeCallback dualWriteStartTimeCallback,
      @Nullable final StateSource stateSource,
      final boolean rollbackDualWriteEnabled) {
    this.rollbackTargets = new ConcurrentHashMap<>(initialRollbackTargets);
    this.fillTargets = new ConcurrentHashMap<>(initialFillTargets);
    this.dualWriteStartRecorded = new ConcurrentHashMap<>();
    this.dualWriteStartTimeCallback = dualWriteStartTimeCallback;
    this.stateSource = stateSource;
    this.rollbackDualWriteEnabled = rollbackDualWriteEnabled;
    if (rollbackTargets.isEmpty()) {
      log.info(
          "Rollback dual-write has no active targets{}",
          stateSource == null ? "" : "; polling for indices swapped after startup");
    } else {
      log.info("Rollback dual-write targets: {}", rollbackTargets.values());
    }
    if (!fillTargets.isEmpty()) {
      log.info(
          "Live deletes are mirrored into the indices these fills are building: {}", fillTargets);
    }
    this.statePoller = stateSource == null ? null : startPoller();
  }

  /**
   * Targets from the persisted Phase 1 state, kept current by polling it. Rollback targets are
   * loaded only when {@code rollbackDualWriteEnabled}; fill targets always.
   */
  @Nonnull
  public static IncrementalReindexWriteTargets fromUpgradeResult(
      @Nonnull final StateSource stateSource,
      @Nullable final DualWriteStartTimeCallback dualWriteStartTimeCallback,
      final boolean rollbackDualWriteEnabled) {
    Map<String, String> phase1State;
    try {
      phase1State = readPhase1State(stateSource).orElse(null);
    } catch (Exception e) {
      log.warn(
          "Could not read the incremental reindex state at startup; starting with no targets and"
              + " polling for them",
          e);
      phase1State = null;
    }
    return new IncrementalReindexWriteTargets(
        rollbackDualWriteEnabled ? rollbackTargetsIn(phase1State) : Map.of(),
        fillTargetsIn(phase1State),
        dualWriteStartTimeCallback,
        stateSource,
        rollbackDualWriteEnabled);
  }

  /**
   * Indices that completed Phase 1 (alias swapped) and recorded their old backing index. Anything
   * else, whether still reindexing, failed, or already {@code DUAL_WRITE_DISABLED}, is not a
   * target.
   */
  @Nonnull
  public static Map<String, RollbackTarget> rollbackTargetsIn(
      @Nullable final Map<String, String> phase1State) {
    final Map<String, RollbackTarget> found = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry :
        IncrementalReindexState.getAllIndexStates(phase1State).entrySet()) {
      final String indexName = entry.getKey();
      final Map<String, String> indexState = entry.getValue();
      final String oldBackingIndexName =
          indexState.get(IncrementalReindexState.OLD_BACKING_INDEX_NAME);
      if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
        continue;
      }
      if (!IncrementalReindexState.Status.COMPLETED
          .name()
          .equals(indexState.get(IncrementalReindexState.STATUS))) {
        continue;
      }
      if (IncrementalReindexState.isOldBackingIndexIdSchemeStale(phase1State, indexName)) {
        log.debug(
            "Index '{}' was rebuilt under a new document-id scheme; '{}' receives no rollback writes",
            indexName,
            oldBackingIndexName);
        continue;
      }
      if (IncrementalReindexState.isOldBackingIndexIncompleteMigration(phase1State, indexName)) {
        log.debug(
            "Index '{}' was rebuilt from an abandoned migration; '{}' receives no rollback"
                + " writes",
            indexName,
            oldBackingIndexName);
        continue;
      }
      found.put(
          indexName,
          new RollbackTarget(
              indexName,
              oldBackingIndexName,
              IncrementalReindexState.getIndexVersion(phase1State, indexName)));
    }
    return found;
  }

  /**
   * Indices whose fill is in flight and writes a physical index other than the alias, keyed by
   * alias. A fill that writes the alias itself needs no mirror: live writes already reach it.
   */
  @Nonnull
  public static Map<String, String> fillTargetsIn(@Nullable final Map<String, String> phase1State) {
    final Map<String, String> found = new HashMap<>();
    for (String indexName : IncrementalReindexState.getAllIndexStates(phase1State).keySet()) {
      if (!IncrementalReindexState.isBackfillStatus(
          IncrementalReindexState.getStatus(phase1State, indexName).orElse(null))) {
        continue;
      }
      IncrementalReindexState.get(
              phase1State, indexName, IncrementalReindexState.BACKFILL_TARGET_INDEX)
          .filter(target -> !target.isEmpty() && !target.equals(indexName))
          .ifPresent(target -> found.put(indexName, target));
    }
    return found;
  }

  public boolean hasRollbackTargets(@Nonnull final EntityIndexVersion indexVersion) {
    return rollbackTargets.values().stream()
        .anyMatch(target -> target.indexVersion() == indexVersion);
  }

  /** Old backing index still receiving rollback writes for the alias {@code indexName}, if any. */
  @Nonnull
  public Optional<String> oldBackingIndexFor(@Nonnull final String indexName) {
    return Optional.ofNullable(rollbackTargets.get(indexName))
        .map(RollbackTarget::oldBackingIndexName);
  }

  /** Index a fill is building for the alias {@code indexName}, while that fill is in flight. */
  @Nonnull
  public Optional<String> fillTargetFor(@Nonnull final String indexName) {
    return Optional.ofNullable(fillTargets.get(indexName));
  }

  /**
   * Records the first rollback write for the alias {@code indexName}, once per index. Phase 2 uses
   * the recorded time as the end of its catch-up window.
   */
  public void recordDualWriteStarted(@Nonnull final String indexName) {
    if (dualWriteStartTimeCallback == null) {
      return;
    }
    final AtomicBoolean recorded =
        dualWriteStartRecorded.computeIfAbsent(indexName, key -> new AtomicBoolean(false));
    if (!recorded.compareAndSet(false, true)) {
      return;
    }
    final long now = System.currentTimeMillis();
    try {
      dualWriteStartTimeCallback.onDualWriteStarted(indexName, now);
      log.info("Recorded dual-write start time for index '{}': {}", indexName, now);
    } catch (Exception e) {
      log.error("Failed to persist dual-write start time for index '{}'", indexName, e);
      recorded.set(false);
    }
  }

  /** Stops rollback writes for the alias {@code indexName}. */
  public void removeRollbackTarget(@Nonnull final String indexName) {
    final RollbackTarget removed = rollbackTargets.remove(indexName);
    if (removed == null) {
      return;
    }
    log.info(
        "Removed rollback dual-write target for index '{}' (was '{}')",
        indexName,
        removed.oldBackingIndexName());
  }

  /** One pass over the persisted state. */
  @VisibleForTesting
  void pollState() {
    if (stateSource == null) {
      return;
    }
    try {
      final Optional<Map<String, String>> read = readPhase1State(stateSource);
      phase1StateRead |= read.isPresent();
      final Map<String, String> phase1State = read.orElse(Map.of());
      if (rollbackDualWriteEnabled) {
        for (RollbackTarget target : rollbackTargetsIn(phase1State).values()) {
          if (rollbackTargets.putIfAbsent(target.indexName(), target) == null) {
            log.info(
                "Added rollback dual-write target for index '{}' swapped after startup (old '{}')",
                target.indexName(),
                target.oldBackingIndexName());
          }
        }
      }
      refreshFillTargets(fillTargetsIn(phase1State));
      for (String indexName : rollbackTargets.keySet()) {
        final boolean disabled =
            IncrementalReindexState.getStatus(phase1State, indexName)
                .filter(IncrementalReindexState.Status.DUAL_WRITE_DISABLED::equals)
                .isPresent();
        if (disabled) {
          removeRollbackTarget(indexName);
        }
      }
    } catch (Exception e) {
      // The targets held so far stay as they are: a failed read says nothing about the state.
      log.warn("Failed to poll incremental reindex state for write targets", e);
    }
  }

  private void refreshFillTargets(@Nonnull final Map<String, String> current) {
    current.forEach(
        (indexName, target) -> {
          if (!target.equals(fillTargets.put(indexName, target))) {
            log.info(
                "Mirroring live deletes for index '{}' into the index its fill is building ('{}')",
                indexName,
                target);
          }
        });
    fillTargets
        .keySet()
        .removeIf(
            indexName -> {
              final boolean gone = !current.containsKey(indexName);
              if (gone) {
                log.info(
                    "Fill for index '{}' is no longer in flight; stopped mirroring live deletes",
                    indexName);
              }
              return gone;
            });
  }

  /**
   * The persisted Phase 1 state, empty when none is persisted. A failed read is thrown, not
   * reported as absent: absence retires fill targets, and a transient failure must not.
   */
  @Nonnull
  private static Optional<Map<String, String>> readPhase1State(
      @Nonnull final StateSource stateSource) throws Exception {
    final DataHubUpgradeResult result =
        DataHubUpgradeResultConditionalPersist.fromEnveloped(
            stateSource
                .upgradeResultStore()
                .readLatest(stateSource.opContext(), stateSource.upgradeIdUrn()));
    if (result == null || result.getResult() == null) {
      return Optional.empty();
    }
    return Optional.of(result.getResult());
  }

  @Nonnull
  private ScheduledExecutorService startPoller() {
    final ScheduledExecutorService poller =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              final Thread thread = new Thread(runnable, "incremental-reindex-state-poller");
              thread.setDaemon(true);
              return thread;
            });
    poller.schedule(this::pollAndReschedule, nextPollDelaySeconds(), TimeUnit.SECONDS);
    return poller;
  }

  private void pollAndReschedule() {
    pollState();
    if (statePoller != null && !statePoller.isShutdown()) {
      statePoller.schedule(this::pollAndReschedule, nextPollDelaySeconds(), TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() {
    if (statePoller != null) {
      statePoller.shutdownNow();
    }
  }

  /**
   * Often until this version's state has been read and while a fill is in flight; rarely otherwise.
   */
  @VisibleForTesting
  long nextPollDelaySeconds() {
    return !phase1StateRead || !fillTargets.isEmpty()
        ? WATCHFUL_POLL_INTERVAL_SECONDS
        : IDLE_POLL_INTERVAL_SECONDS;
  }
}
