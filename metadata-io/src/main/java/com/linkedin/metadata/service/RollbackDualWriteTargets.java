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
 * Old backing indices that must stay current for rollback after an incremental-reindex alias swap.
 *
 * <p>Phase 1 records, per swapped index, the physical index its alias used to point at. Until Phase
 * 2 marks that index {@link IncrementalReindexState.Status#DUAL_WRITE_DISABLED}, every live write
 * to the alias is also applied to the old backing index so the alias can be swapped back. Targets
 * are keyed by the swapped index's alias, the same key Phase 1 state uses, so the V2 and V3 update
 * strategies share one source and each resolves its target from the index it just wrote to.
 *
 * <p>Loaded once at startup and kept current by polling the persisted upgrade state: an index
 * swapped after startup (a fill rebuilt in the non-blocking phase) is added, an index marked
 * DUAL_WRITE_DISABLED is removed. The poller stops once no target remains and no fill is in flight
 * that could still produce one.
 */
@Slf4j
public class RollbackDualWriteTargets {

  private static final long POLL_INTERVAL_SECONDS = 300;

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

  public record Target(
      @Nonnull String indexName,
      @Nonnull String oldBackingIndexName,
      @Nonnull EntityIndexVersion indexVersion) {}

  private final ConcurrentHashMap<String, Target> targets;
  private final ConcurrentHashMap<String, AtomicBoolean> dualWriteStartRecorded;
  @Nullable private final DualWriteStartTimeCallback dualWriteStartTimeCallback;
  @Nullable private final StateSource stateSource;
  @Nullable private final ScheduledExecutorService statePoller;

  public RollbackDualWriteTargets(
      @Nonnull final Map<String, Target> initialTargets,
      @Nullable final DualWriteStartTimeCallback dualWriteStartTimeCallback,
      @Nullable final StateSource stateSource) {
    this.targets = new ConcurrentHashMap<>(initialTargets);
    this.dualWriteStartRecorded = new ConcurrentHashMap<>();
    this.dualWriteStartTimeCallback = dualWriteStartTimeCallback;
    this.stateSource = stateSource;
    if (targets.isEmpty()) {
      log.info(
          "Rollback dual-write has no active targets{}",
          stateSource == null ? "" : "; polling for indices swapped after startup");
    } else {
      log.info("Rollback dual-write targets: {}", targets.values());
    }
    this.statePoller = stateSource == null ? null : startPoller();
  }

  /** Targets from the persisted Phase 1 state, kept current by polling it. */
  @Nonnull
  public static RollbackDualWriteTargets fromUpgradeResult(
      @Nonnull final StateSource stateSource,
      @Nullable final DualWriteStartTimeCallback dualWriteStartTimeCallback) {
    return new RollbackDualWriteTargets(
        targetsIn(readPhase1State(stateSource).orElse(null)),
        dualWriteStartTimeCallback,
        stateSource);
  }

  /**
   * Indices that completed Phase 1 (alias swapped) and recorded their old backing index. Anything
   * else, whether still reindexing, failed, or already {@code DUAL_WRITE_DISABLED}, is not a
   * target.
   */
  @Nonnull
  public static Map<String, Target> targetsIn(@Nullable final Map<String, String> phase1State) {
    final Map<String, Target> found = new HashMap<>();
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
      found.put(
          indexName,
          new Target(
              indexName,
              oldBackingIndexName,
              IncrementalReindexState.getIndexVersion(phase1State, indexName)));
    }
    return found;
  }

  public boolean hasTargets(@Nonnull final EntityIndexVersion indexVersion) {
    return targets.values().stream().anyMatch(target -> target.indexVersion() == indexVersion);
  }

  /** Old backing index still receiving rollback writes for the alias {@code indexName}, if any. */
  @Nonnull
  public Optional<String> oldBackingIndexFor(@Nonnull final String indexName) {
    return Optional.ofNullable(targets.get(indexName)).map(Target::oldBackingIndexName);
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
  public void remove(@Nonnull final String indexName) {
    final Target removed = targets.remove(indexName);
    if (removed == null) {
      return;
    }
    log.info(
        "Removed rollback dual-write target for index '{}' (was '{}')",
        indexName,
        removed.oldBackingIndexName());
  }

  @VisibleForTesting
  /** One pass over the persisted state. Returns whether the poller stays alive. */
  boolean pollState() {
    if (stateSource == null) {
      return false;
    }
    try {
      final Map<String, String> phase1State = readPhase1State(stateSource).orElse(Map.of());
      for (Target target : targetsIn(phase1State).values()) {
        if (targets.putIfAbsent(target.indexName(), target) == null) {
          log.info(
              "Added rollback dual-write target for index '{}' swapped after startup (old '{}')",
              target.indexName(),
              target.oldBackingIndexName());
        }
      }
      for (String indexName : targets.keySet()) {
        final boolean disabled =
            IncrementalReindexState.getStatus(phase1State, indexName)
                .filter(IncrementalReindexState.Status.DUAL_WRITE_DISABLED::equals)
                .isPresent();
        if (disabled) {
          remove(indexName);
        }
      }
      final boolean keepPolling = !targets.isEmpty() || anyFillInFlight(phase1State);
      if (!keepPolling) {
        shutdownPoller();
      }
      return keepPolling;
    } catch (Exception e) {
      log.warn("Failed to poll incremental reindex state for rollback dual-write targets", e);
      return true;
    }
  }

  private static boolean anyFillInFlight(@Nonnull final Map<String, String> phase1State) {
    return IncrementalReindexState.getAllIndexStates(phase1State).keySet().stream()
        .anyMatch(
            indexName ->
                IncrementalReindexState.isBackfillStatus(
                    IncrementalReindexState.getStatus(phase1State, indexName).orElse(null)));
  }

  @Nonnull
  private static Optional<Map<String, String>> readPhase1State(
      @Nonnull final StateSource stateSource) {
    try {
      final DataHubUpgradeResult result =
          DataHubUpgradeResultConditionalPersist.fromEnveloped(
              stateSource
                  .upgradeResultStore()
                  .readLatest(stateSource.opContext(), stateSource.upgradeIdUrn()));
      if (result == null || result.getResult() == null) {
        return Optional.empty();
      }
      return Optional.of(result.getResult());
    } catch (Exception e) {
      log.debug(
          "Could not fetch upgrade result for {}: {}", stateSource.upgradeIdUrn(), e.getMessage());
      return Optional.empty();
    }
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
    poller.scheduleAtFixedRate(
        this::pollState, POLL_INTERVAL_SECONDS, POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
    return poller;
  }

  private void shutdownPoller() {
    if (statePoller != null && !statePoller.isShutdown()) {
      log.info("No rollback dual-write targets remain or can still appear; stopping state poller");
      statePoller.shutdown();
    }
  }
}
